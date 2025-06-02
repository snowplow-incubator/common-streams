/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannel}
import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.grpc.ChannelPoolSettings
import com.google.auth.Credentials
import com.google.cloud.pubsub.v1.{SubscriptionAdminSettings, TopicAdminSettings}
import org.threeten.bp.{Duration => ThreetenDuration}

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.pubsub.sink.PubsubSink
import com.snowplowanalytics.snowplow.streams.pubsub.source.PubsubSource

import java.util.concurrent.{Executors, ScheduledExecutorService}

class PubsubFactory[F[_]: Async] private (
  transport: FixedTransportChannelProvider,
  executor: FixedExecutorProvider
) extends Factory[F, PubsubSourceConfig, PubsubSinkConfig] {

  def sink(config: PubsubSinkConfig): Resource[F, Sink[F]] =
    PubsubSink.resource(config, transport, executor)

  def source(config: PubsubSourceConfig): Resource[F, SourceAndAck[F]] =
    PubsubSource.resource(config, transport, executor)
}

object PubsubFactory {

  def resource[F[_]: Async](config: PubsubFactoryConfig): Resource[F, PubsubFactory[F]] =
    for {
      executor <- executorResource[F]
      channel <- makeChannel(config.gcpUserAgent, executor)
    } yield new PubsubFactory(FixedTransportChannelProvider.create(channel), FixedExecutorProvider.create(executor))

  def makeChannel[F[_]: Sync](gcpUserAgent: GcpUserAgent, executor: ScheduledExecutorService): Resource[F, TransportChannel] = {
    def withCredentials(credentials: Credentials) = Sync[F].delay {
      SubscriptionAdminSettings
        .defaultGrpcTransportProviderBuilder()
        .setMaxInboundMessageSize(20 << 20) // 20 MB
        .setMaxInboundMetadataSize(20 << 20) // 20 MB
        .setKeepAliveTime(ThreetenDuration.ofMinutes(5))
        .setChannelPoolSettings {
          ChannelPoolSettings.staticallySized(1)
        }
        .setEndpoint(SubscriptionAdminSettings.getDefaultEndpoint())
        .setHeaderProvider(GcpUserAgent.headerProvider(gcpUserAgent))
        .setCredentials(credentials)
        .setExecutor(executor)
        .build
    }

    val make = for {
      credentials <- Sync[F].blocking(TopicAdminSettings.defaultCredentialsProviderBuilder().build().getCredentials())
      provider <- withCredentials(credentials)
      channel <- Sync[F].blocking(provider.getTransportChannel)
    } yield channel

    Resource.make(make)(chan => Sync[F].blocking(chan.shutdown))
  }

  private def executorResource[F[_]: Sync]: Resource[F, ScheduledExecutorService] = {
    val make = Sync[F].delay {
      Executors.newScheduledThreadPool(2)
    }
    Resource.make(make)(es => Sync[F].blocking(es.shutdown()))
  }
}
