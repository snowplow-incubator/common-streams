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
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// pubsub
import com.google.api.gax.core.{CredentialsProvider, FixedExecutorProvider}
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.pubsub.v1.ReceivedMessage
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub, SubscriberStubSettings}

// snowplow
import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubSourceConfig

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}

/**
 * A common-streams `Source` that pulls messages from PubSub
 *
 * This Source is a wrapper around a GRPC stub. It can use either "Unary Pull" or "Streaming Pull"
 * GRPC methods to fetch events, based on the `streamingPull` configuration parameter.
 */
private[pubsub] object PubsubSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](
    config: PubsubSourceConfig,
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider,
    credentials: CredentialsProvider
  ): Resource[F, SourceAndAck[F]] =
    for {
      ref <- Resource.eval(Ref[F].of(Map.empty[Unique.Token, PubsubBatchState]))
      stub <- buildSubscriberStub(transport, executor, credentials)
      source <- Resource.eval(LowLevelSource.toSourceAndAck(lowLevel(config, stub, ref)))
    } yield source

  private def lowLevel[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): LowLevelSource[F, Vector[Unique.Token]] =
    new LowLevelSource[F, Vector[Unique.Token]] {
      def checkpointer: Checkpointer[F, Vector[Unique.Token]] = new PubsubCheckpointer(config, stub, refStates)

      def stream: Stream[F, Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
        pubsubStream(config, stub, refStates)

      def debounceCheckpoints: FiniteDuration = Duration.Zero
    }

  private def pubsubStream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): Stream[F, Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
    Stream.emit {
      val source =
        if (config.streamingPull)
          StreamingPullSource.stream(config, stub, refStates)
        else
          UnaryPullSource.stream(config, stub, refStates)

      source
        .concurrently(extendDeadlines(config, stub, refStates))
        .onFinalize(nackRefStatesForShutdown(config, stub, refStates))
    }

  /**
   * Function to be called immediately after receiving pubsub message so we can start managing state
   *
   * Managing state of the batch includes:
   *
   *   - Extend the ack deadline, which gives us some time to process this batch.
   *   - Generate a unique token by which to identify this batch internally
   *   - Add the batch to the local "State" so that we can re-extend the ack deadline if needed
   */
  private[source] def handleReceivedMessages[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    modAckImmediately: Boolean,
    records: Vector[ReceivedMessage]
  ): F[Option[LowLevelEvents[Vector[Unique.Token]]]] =
    if (records.nonEmpty) {
      val chunk  = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
      val ackIds = records.map(_.getAckId)
      for {
        _ <- Logger[F].trace {
               records.map(_.getMessage.getMessageId).mkString("Pubsub message IDs: ", ",", "")
             }
        timeReceived <- Sync[F].realTimeInstant
        _ <- if (modAckImmediately) Utils.modAck[F](config, stub, ackIds, config.durationPerAckExtension) else Sync[F].unit
        token <- Unique[F].unique
        currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
        _ <- refStates.update(_ + (token -> PubsubBatchState(currentDeadline, ackIds)))
      } yield Some(LowLevelEvents(chunk, Vector(token), Some(earliestTimestampOfRecords(records))))
    } else {
      none.pure[F]
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
      Utils.modAck(config, stub, m.values.flatMap(_.ackIds.toVector).toVector, Duration.Zero)
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
          Utils.modAck[F](config, stub, ackIds, config.durationPerAckExtension)
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
    executor: FixedExecutorProvider,
    credentials: CredentialsProvider
  ): Resource[F, GrpcSubscriberStub] = {
    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(executor)
      .setTransportChannelProvider(transport)
      .setCredentialsProvider(credentials)
      .build

    Resource.make(Sync[F].delay(GrpcSubscriberStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

}
