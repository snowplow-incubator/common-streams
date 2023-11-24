/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub

import cats.Monad
import cats.effect.{Async, Resource, Sync}
import cats.effect.implicits._
import cats.effect.kernel.{Deferred, DeferredSink, DeferredSource}
import cats.effect.std._
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
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.{Callable, ExecutorService, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

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

  private case class SingleMessage[F[_]](
    message: ByteBuffer,
    ackReply: AckReplyConsumer,
    tstamp: Instant
  )

  private def pubsubStream[F[_]: Async](config: PubsubSourceConfig): Stream[F, LowLevelEvents[Chunk[AckReplyConsumer]]] = {
    val resources = for {
      dispatcher <- Stream.resource(Dispatcher.sequential(await = false))
      queue <- Stream.eval(Queue.unbounded[F, SingleMessage[F]])
      semaphore <- Stream.eval(Semaphore[F](config.bufferMaxBytes))
      sig <- Stream.eval(Deferred[F, Either[Throwable, Unit]])
      _ <- runSubscriber(config, queue, dispatcher, semaphore, sig)
      _ <- Stream.bracket(Sync[F].unit)(_ => sig.complete(Right(())) *> Sync[F].cede)
    } yield (queue, semaphore, sig)

    resources.flatMap { case (queue, semaphore, sig) =>
      Stream
        .fromQueueUnterminated(queue)
        .chunks
        .filter(_.nonEmpty)
        .map { chunk =>
          val events         = chunk.map(_.message)
          val acks           = chunk.map(_.ackReply)
          val earliestTstamp = chunk.map(_.tstamp).iterator.min
          LowLevelEvents(events, acks, Some(earliestTstamp))
        }
        .evalTap { case LowLevelEvents(events, _, _) =>
          val numPermits = events.foldLeft(0L) { case (numPermits, e) =>
            numPermits + permitsFor(config, e.limit())
          }
          semaphore.releaseN(numPermits)
        }
        .interruptWhen(sig)
    }
  }

  /**
   * Number of semaphore permits needed to write an event to the buffer.
   *
   *   - For small/medium events, this equals the size of the event in bytes.
   *   - For large events, there are not enough permits available for the event in bytes, so return
   *     the number of available permits.
   */
  private def permitsFor(config: PubsubSourceConfig, bytes: Int): Long =
    Math.min(config.bufferMaxBytes, bytes.toLong)

  private def errorListener[F[_]: Sync](dispatcher: Dispatcher[F], sig: DeferredSink[F, Either[Throwable, Unit]]): ApiService.Listener =
    new ApiService.Listener {
      override def failed(from: ApiService.State, failure: Throwable): Unit =
        dispatcher.unsafeRunSync {
          Logger[F].error(failure)("Error from Pubsub subscriber") *>
            sig.complete(Left(failure)).void
        }
    }

  private def runSubscriber[F[_]: Async](
    config: PubsubSourceConfig,
    queue: Queue[F, SingleMessage[F]],
    dispatcher: Dispatcher[F],
    semaphore: Semaphore[F],
    sig: Deferred[F, Either[Throwable, Unit]]
  ): Stream[F, Unit] = {
    val name     = ProjectSubscriptionName.of(config.subscription.projectId, config.subscription.subscriptionId)
    val receiver = messageReceiver(config, queue, dispatcher, semaphore, sig)

    for {
      executorService <- Stream.resource(scheduledExecutorService[F](config))
      subscriber <- Stream.eval(Sync[F].delay {
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
                        .build
                    })
      _ <- Stream.eval(Sync[F].delay {
             subscriber.addListener(errorListener(dispatcher, sig), MoreExecutors.directExecutor)
           })
      _ <- Stream.bracket(Sync[F].delay(subscriber.startAsync())) { apiService =>
             for {
               _ <- Logger[F].info("Stopping the PubSub Subscriber...")
               _ <- Sync[F].delay(apiService.stopAsync())
               _ <- drainQueue(queue)
               _ <- Logger[F].info("Waiing for the PubSub Subscriber to finish cleanly...")
               _ <- Sync[F].blocking(apiService.awaitTerminated())
             } yield ()
           }
    } yield ()
  }

  private def drainQueue[F[_]: Async](queue: QueueSource[F, SingleMessage[F]]): F[Unit] = {

    def go(acc: List[AckReplyConsumer], queue: QueueSource[F, SingleMessage[F]]): F[List[AckReplyConsumer]] =
      queue.tryTake.flatMap {
        case Some(SingleMessage(_, acker, _)) =>
          go(acker :: acc, queue)
        case None =>
          Monad[F].pure(acc)
      }

    go(Nil, queue).flatMap { ackers =>
      pubsubCheckpointer.nack(Chunk.from(ackers))
    }
  }

  private def messageReceiver[F[_]: Async](
    config: PubsubSourceConfig,
    queue: QueueSink[F, SingleMessage[F]],
    dispatcher: Dispatcher[F],
    semaphore: Semaphore[F],
    sig: DeferredSource[F, Either[Throwable, Unit]]
  ): MessageReceiver =
    new MessageReceiver {
      def receiveMessage(message: PubsubMessage, ackReply: AckReplyConsumer): Unit = {
        val tstamp = Instant.ofEpochSecond(message.getPublishTime.getSeconds, message.getPublishTime.getNanos.toLong)
        val put = semaphore.acquireN(permitsFor(config, message.getData.size)) *>
          queue.offer(SingleMessage(message.getData.asReadOnlyByteBuffer(), ackReply, tstamp))

        val io = put
          .race(sig.get)
          .flatMap {
            case Right(_) =>
              Sync[F].delay(ackReply.nack())
            case Left(_) =>
              Sync[F].unit
          }

        dispatcher.unsafeRunSync(io)
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
