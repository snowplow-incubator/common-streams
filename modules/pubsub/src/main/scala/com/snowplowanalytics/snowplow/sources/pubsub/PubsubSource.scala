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
import com.google.api.gax.grpc.ChannelPoolSettings
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber, SubscriptionAdminSettings}
import com.google.common.util.concurrent.{ForwardingExecutorService, ListeningExecutorService, MoreExecutors}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.{
  Callable,
  ExecutorService,
  Executors,
  Phaser,
  ScheduledExecutorService,
  ScheduledFuture,
  Semaphore,
  TimeUnit,
  TimeoutException
}
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
        pubsubStream(config)

      def lastLiveness: F[FiniteDuration] =
        Sync[F].realTime
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

  private def pubsubStream[F[_]: Async](config: PubsubSourceConfig): Stream[F, Stream[F, LowLevelEvents[Chunk[AckReplyConsumer]]]] =
    for {
      control <- Stream.eval(Control.build(config))
      _ <- Stream.resource(runSubscriber(config, control))
    } yield consumeFromQueue(config, control)

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
      .evalMap { _ =>
        // Semantically block until message receive has written at least one message
        val waitForData = Sync[F].interruptible {
          control.phaser.awaitAdvanceInterruptibly(control.phaser.arrive())
        }
        Sync[F].uncancelable { poll =>
          poll(waitForData) *> Sync[F].delay(control.queue.getAndSet(Nil))
        }
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
      direct <- executorResource(Sync[F].delay(MoreExecutors.newDirectExecutorService()))
      parallelPullCount = chooseNumParallelPulls(config)
      channelCount      = chooseNumTransportChannels(config, parallelPullCount)
      executor <- executorResource(Sync[F].delay(Executors.newScheduledThreadPool(2 * parallelPullCount)))
      receiver = messageReceiver(config, control)
      name     = ProjectSubscriptionName.of(config.subscription.projectId, config.subscription.subscriptionId)
      subscriber <- Resource.eval(Sync[F].delay {
                      Subscriber
                        .newBuilder(name, receiver)
                        .setMaxAckExtensionPeriod(convertDuration(config.maxAckExtensionPeriod))
                        .setMaxDurationPerAckExtension(convertDuration(config.maxDurationPerAckExtension))
                        .setMinDurationPerAckExtension(convertDuration(config.minDurationPerAckExtension))
                        .setParallelPullCount(parallelPullCount)
                        .setExecutorProvider(FixedExecutorProvider.create(executorForEventCallbacks(direct, executor)))
                        .setSystemExecutorProvider(FixedExecutorProvider.create(executor))
                        .setFlowControlSettings {
                          // Switch off any flow control, because we handle it ourselves with the semaphore
                          FlowControlSettings.getDefaultInstance
                        }
                        .setHeaderProvider(GcpUserAgent.headerProvider(config.gcpUserAgent))
                        .setChannelProvider {
                          SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder
                            .setMaxInboundMessageSize(20 << 20) // copies Subscriber hard-coded default
                            .setMaxInboundMetadataSize(20 << 20) // copies Subscriber hard-coded default
                            .setKeepAliveTime(ThreetenDuration.ofMinutes(5)) // copies Subscriber hard-coded default
                            .setChannelPoolSettings {
                              ChannelPoolSettings.staticallySized(channelCount)
                            }
                            .build
                        }
                        .build
                    })
      _ <- Resource.eval(Sync[F].delay {
             subscriber.addListener(errorListener(control.phaser, control.errorRef), MoreExecutors.directExecutor)
           })
      apiService <- Resource.make(Sync[F].delay(subscriber.startAsync())) { apiService =>
                      for {
                        _ <- Logger[F].info("Stopping the PubSub Subscriber...")
                        _ <- Sync[F].delay(apiService.stopAsync())
                        fiber <- drainQueue(control).start
                        _ <- Logger[F].info("Waiting for the PubSub Subscriber to finish cleanly...")
                        _ <- Sync[F]
                               .blocking(apiService.awaitTerminated(config.shutdownTimeout.toMillis, TimeUnit.MILLISECONDS))
                               .attemptNarrow[TimeoutException]
                        _ <- Sync[F].delay(control.phaser.forceTermination())
                        _ <- fiber.join
                      } yield ()
                    }
      _ <- Resource.eval(Sync[F].blocking(apiService.awaitRunning()))
    } yield ()

  private def drainQueue[F[_]: Async](control: Control): F[Unit] =
    Async[F].untilDefinedM {
      for {
        _ <- Sync[F].delay(control.semaphore.release(Int.MaxValue - control.semaphore.availablePermits()))
        phase <- Sync[F].blocking(control.phaser.arriveAndAwaitAdvance())
        messages <- Sync[F].delay(control.queue.getAndSet(Nil))
        _ <- pubsubCheckpointer.nack(Chunk.from(messages.map(_.ackReply)))
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
   * The ScheduledExecutorService to be used for processing events.
   *
   * We execute the callback on a `DirectExecutor`, which means the underlying Subscriber runs it
   * directly on its system executor. When the queue is full, this means we deliberately block the
   * system exeuctor. We need to do this trick because we have disabled FlowControl. This trick is
   * our own version of flow control.
   */
  private def executorForEventCallbacks(
    directExecutor: ListeningExecutorService,
    systemExecutor: ScheduledExecutorService
  ): ScheduledExecutorService =
    new ForwardingExecutorService with ScheduledExecutorService {

      /**
       * Non-scheduled tasks (e.g. when a message is received), are run directly, without jumping to
       * another thread pool
       */
      override val delegate = directExecutor

      /**
       * Scheduled tasks (if they exist) are scheduled on the same thread pool shared by the system
       * executor. As far as I know, these schedule methods never get called.
       */
      override def schedule[V](
        callable: Callable[V],
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[V] =
        systemExecutor.schedule(callable, delay, unit)
      override def schedule(
        runnable: Runnable,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        systemExecutor.schedule(runnable, delay, unit)
      override def scheduleAtFixedRate(
        runnable: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        systemExecutor.scheduleAtFixedRate(runnable, initialDelay, period, unit)
      override def scheduleWithFixedDelay(
        runnable: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit
      ): ScheduledFuture[_] =
        systemExecutor.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)
    }

  private def convertDuration(d: FiniteDuration): ThreetenDuration =
    ThreetenDuration.ofMillis(d.toMillis)

  /**
   * Converts `parallelPullFactor` to a suggested number of parallel pulls
   *
   * For bigger instances (more cores) the downstream processor can typically process events more
   * quickly. So the PubSub subscriber needs more parallelism in order to keep downstream saturated
   * with events.
   */
  private def chooseNumParallelPulls(config: PubsubSourceConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.parallelPullFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  /**
   * Picks a sensible number of GRPC transport channels (roughly equivalent to a TCP connection)
   *
   * GRPC has a hard limit of 100 concurrent RPCs on a channel. And experience shows it is healthy
   * to stay much under that limit. If we need to open a large number of streaming pulls then we
   * might approach/exceed that limit.
   */
  private def chooseNumTransportChannels(config: PubsubSourceConfig, parallelPullCount: Int): Int =
    (BigDecimal(parallelPullCount) / config.maxPullsPerTransportChannel)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

}
