/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.kernel.Unique
import cats.implicits._
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// pubsub
import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.pubsub.v1.{PullRequest, PullResponse, ReceivedMessage}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub, SubscriberStubSettings}

// snowplow
import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.pubsub.{FutureInterop, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps.implicits._

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters._

/**
 * A common-streams `Source` that pulls messages from PubSub
 *
 * This Source is a wrapper around a GRPC stub. It uses the "Unary Pull" GRPC method to fetch
 * events.
 *
 * Note that "Unary Pull" GRPC is different to the "Streaming Pull" GRPC used by the 3rd-party
 * java-pubsub library. We use "Unary Pull" to avoid a problem in which PubSub occasionally
 * re-delivers the same messages, causing downstream duplicates. The problem happened especially in
 * apps like Lake Loader, which builds up a very large number of un-acked messages and then acks
 * them all in one go at the end of a timed window.
 */
private[pubsub] object PubsubSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](
    config: PubsubSourceConfig,
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider
  ): Resource[F, SourceAndAck[F]] =
    for {
      ref <- Resource.eval(Ref[F].of(Map.empty[Unique.Token, PubsubBatchState]))
      stub <- buildSubscriberStub(transport, executor)
      source <- Resource.eval(LowLevelSource.toSourceAndAck(lowLevel(config, stub, ref)))
    } yield source

  private def lowLevel[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): LowLevelSource[F, Vector[Unique.Token]] =
    new LowLevelSource[F, Vector[Unique.Token]] {
      def checkpointer: Checkpointer[F, Vector[Unique.Token]] = new PubsubCheckpointer(config.subscription, stub, refStates)

      def stream: Stream[F, Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
        pubsubStream(config, stub, refStates)

      def debounceCheckpoints: FiniteDuration = Duration.Zero
    }

  private def pubsubStream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): Stream[F, Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
    for {
      parallelPullCount <- Stream.eval(Sync[F].delay(chooseNumParallelPulls(config)))
    } yield Stream
      .fixedRateStartImmediately(config.debounceRequests, dampen = true)
      .through(parEvalMapUnorderedOrPrefetch(parallelPullCount)(_ => pullAndManageState(config, stub, refStates)))
      .concurrently(extendDeadlines(config, stub, refStates))
      .onFinalize(nackRefStatesForShutdown(config, stub, refStates))

  private def parEvalMapUnorderedOrPrefetch[F[_]: Async, A, B](parallelPullCount: Int)(f: A => F[B]): Pipe[F, A, B] =
    if (parallelPullCount === 1)
      // If parallelPullCount is 1, we need a prefetch so the behaviour of this source is roughly
      // consistent with other sources and other values of parallePullCount
      _.evalMap(f).prefetch
    else
      // If parallelPullCount > 1, then we don't need prefetch, because parEvalMapUnordered already
      // has the effect of pre-fetching
      _.parEvalMapUnordered(parallelPullCount)(f)

  /**
   * Pulls a batch of messages from pubsub and then manages the state of the batch
   *
   * Managing state of the batch includes:
   *
   *   - Extend the ack deadline, which gives us some time to process this batch.
   *   - Generate a unique token by which to identify this batch internally
   *   - Add the batch to the local "State" so that we can re-extend the ack deadline if needed
   */
  private def pullAndManageState[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): F[Option[LowLevelEvents[Vector[Unique.Token]]]] =
    pullFromSubscription(config, stub).flatMap { response =>
      if (response.getReceivedMessagesCount > 0) {
        val records = response.getReceivedMessagesList.asScala.toVector
        val chunk   = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
        val ackIds  = records.map(_.getAckId)
        Sync[F].uncancelable { _ =>
          for {
            _ <- Logger[F].trace {
                   records.map(_.getMessage.getMessageId).mkString("Pubsub message IDs: ", ",", "")
                 }
            timeReceived <- Sync[F].realTimeInstant
            _ <- Utils.modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension)
            token <- Unique[F].unique
            currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
            _ <- refStates.update(_ + (token -> PubsubBatchState(currentDeadline, ackIds)))
          } yield Some(LowLevelEvents(chunk, Vector(token), Some(earliestTimestampOfRecords(records))))
        }
      } else {
        none.pure[F]
      }
    }

  private def earliestTimestampOfRecords(records: Vector[ReceivedMessage]): FiniteDuration = {
    val (tstampSeconds, tstampNanos) =
      records.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
    tstampSeconds.seconds + tstampNanos.toLong.nanos
  }

  /**
   * "Nacks" any message that was pulled from pubsub but never consumed by the app.
   *
   * This is called during graceful shutdown. It allows PubSub to immediately re-deliver the
   * messages to a different pod; instead of waiting for the ack deadline to expire.
   */
  private def nackRefStatesForShutdown[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): F[Unit] =
    refStates.getAndSet(Map.empty).flatMap { m =>
      Utils.modAck(config.subscription, stub, m.values.flatMap(_.ackIds.toVector).toVector, Duration.Zero)
    }

  /**
   * Wrapper around the "Pull" PubSub GRPC.
   *
   * @return
   *   The PullResponse, comprising a batch of pubsub messages
   */
  private def pullFromSubscription[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub
  ): F[PullResponse] = {
    val request = PullRequest.newBuilder
      .setSubscription(config.subscription.show)
      .setMaxMessages(config.maxMessagesPerPull)
      .build
    val io = for {
      apiFuture <- Sync[F].delay(stub.pullCallable.futureCall(request))
      res <- FutureInterop.fromFuture[F, PullResponse](apiFuture)
    } yield res
    Logger[F].debug("Pulling from subscription") *>
      io.retryingOnTransientGrpcFailures
        .flatTap { response =>
          Logger[F].debug(s"Pulled ${response.getReceivedMessagesCount} messages")
        }
  }

  /**
   * Modify ack deadlines if we need more time to process the messages
   *
   * @param config
   *   The Source configuration
   * @param stub
   *   The GRPC stub on which we can issue modack requests
   * @param refStates
   *   A map from tokens to the data held about a batch of messages received from pubsub. This
   *   function must update the state if it extends a deadline.
   */
  private def extendDeadlines[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): Stream[F, Nothing] =
    Stream
      .eval(Sync[F].realTimeInstant)
      .evalMap { now =>
        val minAllowedDeadline = now.plusMillis((config.minRemainingAckDeadline.toDouble * config.durationPerAckExtension.toMillis).toLong)
        val newDeadline        = now.plusMillis(config.durationPerAckExtension.toMillis)
        refStates.modify { m =>
          val toExtend = m.filter { case (_, batchState) =>
            batchState.currentDeadline.isBefore(minAllowedDeadline)
          }
          val fixed = toExtend.view.map { case (k, v) =>
            k -> v.copy(currentDeadline = newDeadline)
          }.toMap
          (m ++ fixed, toExtend.values.toVector)
        }
      }
      .evalMap { toExtend =>
        if (toExtend.isEmpty)
          // If no message had a deadline close to expiry, then sleep for an appropriate amount of time and check again
          Sync[F].sleep(0.5 * config.minRemainingAckDeadline.toDouble * config.durationPerAckExtension)
        else {
          val ackIds = toExtend.sortBy(_.currentDeadline).flatMap(_.ackIds)
          Utils.modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension)
        }
      }
      .repeat
      .drain

  /**
   * Builds the "Stub" which is the object from which we can call PubSub SDK methods
   *
   * This implementation has some hard-coded values, which have been copied over from the equivalent
   * hard-coded values in the java-pubsub client library.
   */
  private def buildSubscriberStub[F[_]: Sync](
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider
  ): Resource[F, GrpcSubscriberStub] = {
    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(executor)
      .setTransportChannelProvider(transport)
      .build

    Resource.make(Sync[F].delay(GrpcSubscriberStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

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

}
