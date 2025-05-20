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
import cats.effect.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.grpc.ChannelPoolSettings
import com.google.cloud.pubsub.v1.stub.{GrpcPublisherStub, PublisherStub, PublisherStubSettings}
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.pubsub.v1.{PublishRequest, PubsubMessage}
import com.google.protobuf.UnsafeSnowplowOps

import com.snowplowanalytics.snowplow.pubsub.FutureInterop
import com.snowplowanalytics.snowplow.pubsub.PubsubRetryOps.implicits._
import com.snowplowanalytics.snowplow.sinks.{ListOfList, Sink, Sinkable}

import scala.jdk.CollectionConverters._
import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent

object PubsubSink {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](config: PubsubSinkConfig): Resource[F, Sink[F]] =
    stubResource[F](config).map { stub =>
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

  private def stubResource[F[_]: Async](
    config: PubsubSinkConfig
  ): Resource[F, PublisherStub] =
    for {
      executor <- executorResource
      subStub <- buildPublisherStub(config, executor)
    } yield subStub

  private def executorResource[F[_]: Sync]: Resource[F, ScheduledExecutorService] = {
    val make = Sync[F].delay {
      Executors.newSingleThreadScheduledExecutor
    }
    Resource.make(make)(es => Sync[F].blocking(es.shutdown()))
  }

  /**
   * Builds the "Stub" which is the object from which we can call PubSub SDK methods
   */
  private def buildPublisherStub[F[_]: Sync](
    config: PubsubSinkConfig,
    executor: ScheduledExecutorService
  ): Resource[F, GrpcPublisherStub] = {
    val channelProvider = TopicAdminSettings
      .defaultGrpcTransportProviderBuilder()
      .setChannelPoolSettings {
        ChannelPoolSettings.staticallySized(1)
      }
      .build

    val stubSettings = PublisherStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(FixedExecutorProvider.create(executor))
      .setCredentialsProvider(TopicAdminSettings.defaultCredentialsProviderBuilder().build())
      .setTransportChannelProvider(channelProvider)
      .setHeaderProvider(GcpUserAgent.headerProvider(config.gcpUserAgent))
      .setEndpoint(PublisherStubSettings.getDefaultEndpoint())
      .build

    Resource.make(Sync[F].delay(GrpcPublisherStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

}
