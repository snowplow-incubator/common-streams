/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.implicits._
import cats.effect.kernel.Unique
import cats.effect.{Async, Ref, Sync}
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.AcknowledgeRequest
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration

import com.snowplowanalytics.snowplow.streams.internal.Checkpointer
import com.snowplowanalytics.snowplow.streams.pubsub.{FutureInterop, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps.implicits._

/**
 * The Pubsub checkpointer
 *
 * @param subscription
 *   Pubsub subscription name
 * @param stub
 *   The GRPC stub needed to execute the ack/nack RPCs
 * @param refState
 *   A map from tokens to the data held about a batch of messages received from pubsub. The map is
 *   wrapped in a `Ref` because it is concurrently modified by the source adding new batches to the
 *   state.
 */
private class PubsubCheckpointer[F[_]: Async](
  subscription: PubsubSourceConfig.Subscription,
  stub: SubscriberStub,
  refAckIds: Ref[F, Map[Unique.Token, PubsubBatchState]]
) extends Checkpointer[F, Vector[Unique.Token]] {

  private implicit def logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def combine(x: Vector[Unique.Token], y: Vector[Unique.Token]): Vector[Unique.Token] =
    x |+| y

  override val empty: Vector[Unique.Token] = Vector.empty

  /**
   * Ack some batches of messages received from pubsub
   *
   * @param c
   *   tokens which are keys to batch data held in the shared state
   */
  override def ack(c: Vector[Unique.Token]): F[Unit] =
    for {
      ackDatas <- refAckIds.modify(m => (m -- c, c.flatMap(m.get)))
      _ <- ackDatas.flatMap(_.ackIds).grouped(1000).toVector.traverse_ { ackIds =>
             val request = AcknowledgeRequest.newBuilder.setSubscription(subscription.show).addAllAckIds(ackIds.asJava).build
             val attempt = for {
               apiFuture <- Sync[F].delay(stub.acknowledgeCallable.futureCall(request))
               _ <- FutureInterop.fromFuture[F, com.google.protobuf.Empty](apiFuture)
             } yield ()
             attempt.retryingOnTransientGrpcFailures
               .recoveringOnGrpcInvalidArgument { s =>
                 // This can happen if ack IDs have expired before we acked
                 Logger[F].info(s"Ignoring error from GRPC when acking: ${s.getDescription}")
               }
           }
    } yield ()

  /**
   * Nack some batches of messages received from pubsub
   *
   * @param c
   *   tokens which are keys to batch data held in the shared state
   */
  override def nack(c: Vector[Unique.Token]): F[Unit] =
    for {
      ackDatas <- refAckIds.modify(m => (m -- c, c.flatMap(m.get)))
      ackIds = ackDatas.flatMap(_.ackIds)
      // A nack is just a modack with zero duration
      _ <- Utils.modAck[F](subscription, stub, ackIds, Duration.Zero)
    } yield ()
}
