/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.pubsub

import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import cats.Foldable
import com.google.api.core.{ApiFuture, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.UnsafeSnowplowOps
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import com.snowplowanalytics.snowplow.pubsub.FutureInterop
import com.snowplowanalytics.snowplow.sinks.{ListOfList, Sink, Sinkable}
import org.threeten.bp.{Duration => ThreetenDuration}

import scala.jdk.CollectionConverters._
import java.util.UUID

import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent

object PubsubSink {

  def resource[F[_]: Async](config: PubsubSinkConfig): Resource[F, Sink[F]] =
    mkPublisher[F](config).map { p =>
      Sink(sinkBatch[F](p, _))
    }

  private def sinkBatch[F[_]: Async](publisher: Publisher, batch: ListOfList[Sinkable]): F[Unit] =
    Foldable[ListOfList]
      .foldM(batch, List.empty[ApiFuture[String]]) { case (futures, Sinkable(bytes, _, attributes)) =>
        for {
          uuid <- Async[F].delay(UUID.randomUUID)
          message = PubsubMessage.newBuilder
                      .setData(UnsafeSnowplowOps.wrapBytes(bytes))
                      .setMessageId(uuid.toString)
                      .putAllAttributes(attributes.asJava)
                      .build
          fut <- Async[F].delay(publisher.publish(message))
        } yield fut :: futures
      }
      .flatMap { futures =>
        for {
          _ <- Async[F].delay(publisher.publishAllOutstanding)
          combined = ApiFutures.allAsList(futures.asJava)
          _ <- FutureInterop.fromFuture(combined)
        } yield ()
      }

  private def mkPublisher[F[_]: Sync](config: PubsubSinkConfig): Resource[F, Publisher] = {
    val topic = ProjectTopicName.of(config.topic.projectId, config.topic.topicId)

    val batchSettings = BatchingSettings.newBuilder
      .setElementCountThreshold(config.batchSize)
      .setRequestByteThreshold(config.requestByteThreshold)
      .setDelayThreshold(ThreetenDuration.ofNanos(Long.MaxValue))

    val make = Sync[F].delay {
      Publisher
        .newBuilder(topic)
        .setBatchingSettings(batchSettings.build)
        .setHeaderProvider(GcpUserAgent.headerProvider(config.gcpUserAgent))
        .build
    }

    Resource.make(make) { publisher =>
      Sync[F].blocking {
        publisher.shutdown()
      }
    }
  }
}
