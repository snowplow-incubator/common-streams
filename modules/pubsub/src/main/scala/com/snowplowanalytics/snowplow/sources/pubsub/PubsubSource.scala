/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub

import cats.Monad
import cats.effect.{Async, Sync}
import cats.effect.implicits._
import cats.effect.kernel.{Deferred, DeferredSink, DeferredSource}
import cats.effect.std._
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer
import java.time.Instant

// pubsub
import com.google.api.core.ApiService
import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.core.ExecutorProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.{ForwardingListeningExecutorService, MoreExecutors}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.{Callable, ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

object PubsubSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: PubsubSourceConfig): F[SourceAndAck[F]] =
    LowLevelSource.toSourceAndAck(lowLevel(config))

  private type PubSubCheckpointer[F[_]] = Checkpointer[F, List[AckReplyConsumer]]

  private def lowLevel[F[_]: Async](config: PubsubSourceConfig): LowLevelSource[F, List[AckReplyConsumer]] =
    new LowLevelSource[F, List[AckReplyConsumer]] {
      def checkpointer: PubSubCheckpointer[F] = pubsubCheckpointer

      def stream: Stream[F, Stream[F, LowLevelEvents[List[AckReplyConsumer]]]] =
        Stream.emit(pubsubStream(config))
    }

  private def pubsubCheckpointer[F[_]: Async]: PubSubCheckpointer[F] = new PubSubCheckpointer[F] {
    def combine(x: List[AckReplyConsumer], y: List[AckReplyConsumer]): List[AckReplyConsumer] =
      x ::: y

    val empty: List[AckReplyConsumer] = Nil
    def ack(c: List[AckReplyConsumer]): F[Unit] =
      Sync[F].delay {
        c.foreach(_.ack())
      }

    def nack(c: List[AckReplyConsumer]): F[Unit] =
      Sync[F].delay {
        c.foreach(_.nack())
      }
  }

  private case class SingleMessage[F[_]](
    message: ByteBuffer,
    ackReply: AckReplyConsumer,
    tstamp: Instant
  )

  private def pubsubStream[F[_]: Async](config: PubsubSourceConfig): Stream[F, LowLevelEvents[List[AckReplyConsumer]]] = {
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
          val events         = chunk.map(_.message).toList
          val acks           = chunk.map(_.ackReply).toList
          val earliestTstamp = chunk.map(_.tstamp).iterator.min
          LowLevelEvents(events, acks, Some(earliestTstamp))
        }
        .evalTap { case LowLevelEvents(events, _, _) =>
          val numPermits = events.map(e => permitsFor(config, e.limit())).sum
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
      executor <- Stream.bracket(Sync[F].delay(scheduledExecutorService))(s => Sync[F].delay(s.shutdown()))
      subscriber <- Stream.eval(Sync[F].delay {
                      Subscriber
                        .newBuilder(name, receiver)
                        .setMaxAckExtensionPeriod(convertDuration(config.maxAckExtensionPeriod))
                        .setMaxDurationPerAckExtension(convertDuration(config.maxDurationPerAckExtension))
                        .setMinDurationPerAckExtension(convertDuration(config.minDurationPerAckExtension))
                        .setParallelPullCount(config.parallelPullCount)
                        .setExecutorProvider {
                          new ExecutorProvider {
                            def shouldAutoClose: Boolean              = true
                            def getExecutor: ScheduledExecutorService = executor
                          }
                        }
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
      pubsubCheckpointer.ack(ackers)
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

  private def scheduledExecutorService: ScheduledExecutorService = new ForwardingListeningExecutorService with ScheduledExecutorService {
    val delegate       = MoreExecutors.newDirectExecutorService
    lazy val scheduler = new ScheduledThreadPoolExecutor(1) // I think this scheduler is never used, but I implement it here for safety
    override def schedule[V](
      callable: Callable[V],
      delay: Long,
      unit: TimeUnit
    ): ScheduledFuture[V] =
      scheduler.schedule(callable, delay, unit)
    override def schedule(
      runnable: Runnable,
      delay: Long,
      unit: TimeUnit
    ): ScheduledFuture[_] =
      scheduler.schedule(runnable, delay, unit)
    override def scheduleAtFixedRate(
      runnable: Runnable,
      initialDelay: Long,
      period: Long,
      unit: TimeUnit
    ): ScheduledFuture[_] =
      scheduler.scheduleAtFixedRate(runnable, initialDelay, period, unit)
    override def scheduleWithFixedDelay(
      runnable: Runnable,
      initialDelay: Long,
      delay: Long,
      unit: TimeUnit
    ): ScheduledFuture[_] =
      scheduler.scheduleWithFixedDelay(runnable, initialDelay, delay, unit)
    override def shutdown(): Unit = {
      delegate.shutdown()
      scheduler.shutdown()
    }
  }

  private def convertDuration(d: FiniteDuration): ThreetenDuration =
    ThreetenDuration.ofMillis(d.toMillis)
}
