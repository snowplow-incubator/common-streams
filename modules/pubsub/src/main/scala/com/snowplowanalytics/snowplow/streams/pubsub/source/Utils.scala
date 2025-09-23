/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.{Async, Sync}
import cats.implicits._
import org.typelevel.log4cats.Logger

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps.implicits._
import com.snowplowanalytics.snowplow.streams.pubsub.{FutureInterop, PubsubSourceConfig}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

private[source] object Utils {

  def modAck[F[_]: Async: Logger](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    ackIds: Vector[String],
    duration: FiniteDuration
  ): F[Unit] =
    ackIds.grouped(1000).toVector.traverse_ { group =>
      val request = ModifyAckDeadlineRequest.newBuilder
        .setSubscription(config.subscription.show)
        .addAllAckIds(group.asJava)
        .setAckDeadlineSeconds(duration.toSeconds.toInt)
        .build
      val io = for {
        _ <- Logger[F].debug(s"Modifying ack deadline for ${ackIds.length} messages by ${duration.toSeconds} seconds")
        apiFuture <- Sync[F].delay(stub.modifyAckDeadlineCallable.futureCall(request))
        _ <- FutureInterop.fromFuture_(apiFuture)
      } yield ()

      io.retryingOnTransientGrpcFailures(config.retries.transientErrors.delay, config.retries.transientErrors.attempts)
        .recoveringOnGrpcInvalidArgument { s =>
          // This can happen if ack IDs were acked before we modAcked
          Logger[F].info(s"Ignoring error from GRPC when modifying ack IDs: ${s.getDescription}")
        }
    }

}
