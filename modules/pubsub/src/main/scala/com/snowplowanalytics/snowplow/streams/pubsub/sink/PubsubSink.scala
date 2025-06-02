/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.sink

import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcPublisherStub, PublisherStub, PublisherStubSettings}
import com.google.pubsub.v1.{PublishRequest, PubsubMessage}
import com.google.protobuf.UnsafeSnowplowOps

import com.snowplowanalytics.snowplow.streams.pubsub.{FutureInterop, PubsubSinkConfig}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps.implicits._
import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}

import scala.jdk.CollectionConverters._

private[pubsub] object PubsubSink {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](
    config: PubsubSinkConfig,
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider
  ): Resource[F, Sink[F]] =
    buildPublisherStub[F](transport, executor).map { stub =>
      Sink(sinkBatch[F](config, stub, _))
    }

  private def sinkBatch[F[_]: Async](
    config: PubsubSinkConfig,
    stub: PublisherStub,
    batch: ListOfList[Sinkable]
  ): F[Unit] =
    batch
      .mapUnordered { case Sinkable(bytes, _, attributes) =>
        PubsubMessage.newBuilder
          .setData(UnsafeSnowplowOps.wrapBytes(bytes))
          .putAllAttributes(attributes.asJava)
          .build
      }
      .group(config.batchSize, config.requestByteThreshold, _.getSerializedSize())
      .parTraverse_ { messages =>
        val request = PublishRequest.newBuilder
          .setTopic(s"projects/${config.topic.projectId}/topics/${config.topic.topicId}")
          .addAllMessages(messages.asJava)
          .build
        val io = for {
          apiFuture <- Sync[F].delay(stub.publishCallable.futureCall(request))
          _ <- FutureInterop.fromFuture_(apiFuture)
        } yield ()
        io.retryingOnTransientGrpcFailures
      }

  /**
   * Builds the "Stub" which is the object from which we can call PubSub SDK methods
   */
  private def buildPublisherStub[F[_]: Sync](
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider
  ): Resource[F, GrpcPublisherStub] = {
    val stubSettings = PublisherStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(executor)
      .setTransportChannelProvider(transport)
      .build

    Resource.make(Sync[F].delay(GrpcPublisherStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

}
