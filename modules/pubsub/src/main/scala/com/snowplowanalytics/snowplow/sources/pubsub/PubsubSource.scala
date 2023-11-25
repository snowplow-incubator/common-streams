/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub

import cats.effect.{Async, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer
import java.time.Instant

// pubsub
import com.google.api.core.ApiService
import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.core.FixedExecutorProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.{ForwardingExecutorService, MoreExecutors}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.{Callable, ExecutorService, Executors, Phaser, ScheduledExecutorService, ScheduledFuture, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

object PubsubSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: PubsubSourceConfig): F[SourceAndAck[F]] =
    LowLevelSource.toSourceAndAck(lowLevel(config))

  private type PubSubCheckpointer[F[_]] = Checkpointer[F, Chunk[AckReplyConsumer]]

  private def lowLevel[F[_]: Async](config: PubsubSourceConfig): LowLevelSource[F, Chunk[AckReplyConsumer]] =
    new LowLevelSource[F, Chunk[AckReplyConsumer]] {
      def checkpointer: PubSubCheckpointer[F] = pubsubCheckpointer

      def stream: Stream[F, Stream[F, LowLevelEvents[Chunk[AckReplyConsumer]]]] =
        Stream.emit(pubsubStream(config))
    }

  private def pubsubCheckpointer[F[_]: Async]: PubSubCheckpointer[F] = new PubSubCheckpointer[F] {
    def combine(x: Chunk[AckReplyConsumer], y: Chunk[AckReplyConsumer]): Chunk[AckReplyConsumer] =
      Chunk.Queue(x, y)

    val empty: Chunk[AckReplyConsumer] = Chunk.empty
    def ack(c: Chunk[AckReplyConsumer]): F[Unit] =
      Sync[F].delay {
        c.foreach(_.ack())
      }

    def nack(c: Chunk[AckReplyConsumer]): F[Unit] =
      Sync[F].delay {
        c.foreach(_.nack())
      }
  }

  private case class SingleMessage(
    message: ByteBuffer,
    ackReply: AckReplyConsumer,
    tstamp: Instant
  )

  /**
   * The toolkit for coordinating concurrent operations between the message receiver and the fs2
   * stream. This is the interface between FS2 and non-FS2 worlds.
   *
   * We use pure Java (non-cats-effect) classes so we can use them in the message receiver without
   * needing to use a cats-effect Dispatcher.
   *
   * @param queue
   *   A List of messages that have been provided by the Pubsub Subscriber. The FS2 stream should
   *   periodically drain this queue.
   * @param semaphore
   *   A Semaphore used to manage how many bytes are being held in memory. When the queue holds too
   *   many bytes, the semaphore will block on acquiring more permits. This is needed because we
   *   have turned off FlowControl in the subscriber.
   * @param phaser
   *   A Phaser that advances each time the queue has new messages to be consumed. The FS2 stream
   *   can wait on the phaser instead of repeatedly pooling the queue.
   * @param errorRef
   *   Any error provided to us by the pubsub Subscriber. If the FS2 stream sees an error here, then
   *   it should raise the error.
   */
  private case class Control(
    queue: AtomicReference[List[SingleMessage]],
    semaphore: Semaphore,
    phaser: Phaser,
    errorRef: AtomicReference[Option[Throwable]]
  )

  private object Control {
    def build[F[_]: Sync](config: PubsubSourceConfig): F[Control] = Sync[F].delay {
      Control(
        new AtomicReference(Nil),
        new Semaphore(config.bufferMaxBytes, false),
        new Phaser(2),
        new AtomicReference(None)
      )
    }
  }

  private def pubsubStream[F[_]: Async](config: PubsubSourceConfig): Stream[F, LowLevelEvents[Chunk[AckReplyConsumer]]] =
    for {
      control <- Stream.eval(Control.build(config))
      _ <- Stream.resource(runSubscriber(config, control))
      events <- consumeFromQueue(config, control)
    } yield events

  private def consumeFromQueue[F[_]: Sync](
    config: PubsubSourceConfig,
    control: Control
  ): Stream[F, LowLevelEvents[Chunk[AckReplyConsumer]]] =
    Stream
      .repeatEval {
        Sync[F].delay(control.errorRef.get).flatMap {
          case None            => Sync[F].unit
          case Some(throwable) => Sync[F].raiseError[Unit](throwable)
        }
      }
      .evalTap { _ =>
        // Semantically block until message receive has written at least one message
        Sync[F].interruptible {
          control.phaser.awaitAdvanceInterruptibly(control.phaser.arrive())
        }
      }
      .evalMap { _ =>
        // Drain the queue and reset it to be empty
        Sync[F].delay(control.queue.getAndSet(Nil))
      }
      .filter(_.nonEmpty) // Would happen if phaser was terminated
      .map { list =>
        val events         = Chunk.iterator(list.iterator.map(_.message))
        val acks           = Chunk.iterator(list.iterator.map(_.ackReply))
        val earliestTstamp = list.iterator.map(_.tstamp).min
        LowLevelEvents(events, acks, Some(earliestTstamp))
      }
      .evalTap { case LowLevelEvents(events, _, _) =>
        val numPermits = events.foldLeft(0) { case (numPermits, e) =>
          numPermits + permitsFor(config, e.remaining())
        }
        Sync[F].delay {
          control.semaphore.release(numPermits)
        }
      }

  /**
   * Number of semaphore permits needed to write an event to the buffer.
   *
   *   - For small/medium events, this equals the size of the event in bytes.
   *   - For large events, there are not enough permits available for the event in bytes, so return
   *     the number of available permits.
   */
  private def permitsFor(config: PubsubSourceConfig, bytes: Int): Int =
    Math.min(config.bufferMaxBytes, bytes)

  private def errorListener(phaser: Phaser, errorRef: AtomicReference[Option[Throwable]]): ApiService.Listener =
    new ApiService.Listener {
      override def failed(from: ApiService.State, failure: Throwable): Unit = {
        errorRef.compareAndSet(None, Some(failure))
        phaser.forceTermination()
      }
    }

  private def runSubscriber[F[_]: Async](config: PubsubSourceConfig, control: Control): Resource[F, Unit] =
    for {
      executorService <- scheduledExecutorService[F](config)
      receiver = messageReceiver(config, control)
      name     = ProjectSubscriptionName.of(config.subscription.projectId, config.subscription.subscriptionId)
      subscriber <- Resource.eval(Sync[F].delay {
                      Subscriber
                        .newBuilder(name, receiver)
                        .setMaxAckExtensionPeriod(convertDuration(config.maxAckExtensionPeriod))
                        .setMaxDurationPerAckExtension(convertDuration(config.maxDurationPerAckExtension))
                        .setMinDurationPerAckExtension(convertDuration(config.minDurationPerAckExtension))
                        .setParallelPullCount(config.parallelPullCount)
                        .setExecutorProvider(FixedExecutorProvider.create(executorService))
                        .setSystemExecutorProvider(FixedExecutorProvider.create(executorService))
                        .setFlowControlSettings {
                          // Switch off any flow control, because we handle it ourselves with the semaphore
                          FlowControlSettings.getDefaultInstance
                        }
                        .setHeaderProvider(GcpUserAgent.headerProvider(config.gcpUserAgent))
                        .build
                    })
      _ <- Resource.eval(Sync[F].delay {
             subscriber.addListener(
               errorListener(control.phaser, control.errorRef),
               MoreExecutors.directExecutor // TODO: use the non-blocking executor for errors?
             )
           })
      _ <- Resource.make(Sync[F].delay(subscriber.startAsync())) { apiService =>
             for {
               _ <- Logger[F].info("Stopping the PubSub Subscriber...")
               _ <- Sync[F].delay(apiService.stopAsync())
               fiber <- drainQueue(control).start
               _ <- Logger[F].info("Waiting for the PubSub Subscriber to finish cleanly...")
               _ <- Sync[F].blocking(apiService.awaitTerminated())
               _ <- Sync[F].delay(control.phaser.forceTermination())
               _ <- fiber.join
             } yield ()
           }
    } yield ()

  private def drainQueue[F[_]: Async](control: Control): F[Unit] =
    Async[F].untilDefinedM {
      for {
        phase <- Sync[F].blocking(control.phaser.arriveAndAwaitAdvance())
        messages <- Sync[F].delay(control.queue.getAndSet(Nil))
        _ <- pubsubCheckpointer.nack(Chunk.from(messages.map(_.ackReply)))
        _ <- Sync[F].delay(control.semaphore.release(Int.MaxValue - control.semaphore.availablePermits()))
      } yield if (phase < 0) None else Some(())
    }

  private def messageReceiver(
    config: PubsubSourceConfig,
    control: Control
  ): MessageReceiver =
    new MessageReceiver {
      def receiveMessage(message: PubsubMessage, ackReply: AckReplyConsumer): Unit = {
        val tstamp          = Instant.ofEpochSecond(message.getPublishTime.getSeconds, message.getPublishTime.getNanos.toLong)
        val singleMessage   = SingleMessage(message.getData.asReadOnlyByteBuffer(), ackReply, tstamp)
        val permitsRequired = permitsFor(config, singleMessage.message.remaining())
        control.semaphore.acquire(permitsRequired)
        val previousQueue = control.queue.getAndUpdate(list => singleMessage :: list)
        if (previousQueue.isEmpty) {
          control.phaser.arrive()
        }
        ()
      }
    }

  private def executorResource[F[_]: Sync, E <: ExecutorService](make: F[E]): Resource[F, E] =
    Resource.make(make)(es => Sync[F].blocking(es.shutdown()))

  /**
   * Source operations are backed by two thread pools:
   *
   *   - A single thread pool, which is only even used for maintenance tasks like extending ack
   *     extension periods.
   *   - A small fixed-sized thread pool on which we deliberately run blocking tasks, like
   *     attempting to write to the Queue.
   *
   * Because of this separation, even when the Queue is full, it cannot block the subscriber's
   * maintenance tasks.
   *
   * Note, we use the same exact same thread pools for the subscriber's `executorProvider` and
   * `systemExecutorProvider`. This means when the Queue is full then it blocks the subscriber from
   * fetching more messages from pubsub. We need to do this trick because we have disabled flow
   * control.
   */
  private def scheduledExecutorService[F[_]: Sync](config: PubsubSourceConfig): Resource[F, ScheduledExecutorService] =
    for {
      forMaintenance <- executorResource(Sync[F].delay(Executors.newSingleThreadScheduledExecutor))
      forBlocking <- executorResource(Sync[F].delay(Executors.newFixedThreadPool(config.parallelPullCount)))
    } yield new ForwardingExecutorService with ScheduledExecutorService {

      /**
       * Any callable/runnable which is **scheduled** must be a maintenance task, so run it on the
       * dedicated maintenance pool
       */
      override def schedule[V](
        callable: Callable[V],
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[V] =
        forMaintenance.schedule(callable, delay, unit)
      override def schedule(
        runnable: Runnable,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.schedule(runnable, delay, unit)
      override def scheduleAtFixedRate(
        runnable: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.scheduleAtFixedRate(runnable, initialDelay, period, unit)
      override def scheduleWithFixedDelay(
        runnable: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        forMaintenance.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)

      /**
       * Non-scheduled tasks (e.g. when a message is received), can be run on the fixed-size
       * blocking pool
       */
      override val delegate = forBlocking
    }

  private def convertDuration(d: FiniteDuration): ThreetenDuration =
    ThreetenDuration.ofMillis(d.toMillis)
}
