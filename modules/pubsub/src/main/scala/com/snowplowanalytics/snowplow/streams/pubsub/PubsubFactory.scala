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
import io.grpc.ManagedChannelBuilder
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannel}
import com.google.api.gax.core.{CredentialsProvider, FixedCredentialsProvider, FixedExecutorProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.{ChannelPoolSettings, GrpcTransportChannel}
import com.google.cloud.pubsub.v1.{SubscriptionAdminSettings, TopicAdminSettings}
import org.threeten.bp.{Duration => ThreetenDuration}

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.pubsub.sink.PubsubSink
import com.snowplowanalytics.snowplow.streams.pubsub.source.PubsubSource

import java.util.concurrent.{Executors, ScheduledExecutorService}

class PubsubFactory[F[_]: Async] private (
  transport: FixedTransportChannelProvider,
  executor: FixedExecutorProvider,
  credentials: CredentialsProvider
) extends Factory[F, PubsubSourceConfig, PubsubSinkConfig] {

  def sink(config: PubsubSinkConfig): Resource[F, Sink[F]] =
    PubsubSink.resource(config, transport, executor, credentials)

  def source(config: PubsubSourceConfig): Resource[F, SourceAndAck[F]] =
    PubsubSource.resource(config, transport, executor, credentials)
}

object PubsubFactory {

  def resource[F[_]: Async](config: PubsubFactoryConfig): Resource[F, PubsubFactory[F]] =
    for {
      executor <- executorResource[F]
      credentialsProvider <- Resource.eval(makeCredentialsProvider[F](config))
      channel <- makeChannel(config, executor)
    } yield new PubsubFactory(FixedTransportChannelProvider.create(channel), FixedExecutorProvider.create(executor), credentialsProvider)

  def makeCredentialsProvider[F[_]: Sync](config: PubsubFactoryConfig): F[CredentialsProvider] =
    config.emulatorHost match {
      case Some(_) =>
        // The pubsub emulator is enabled, so do not try to resolve credentials
        Sync[F].pure(NoCredentialsProvider.create())
      case None =>
        // Pubsub emulator _not_ enabled. Use real credentials for real pubsub
        Sync[F]
          .blocking(TopicAdminSettings.defaultCredentialsProviderBuilder().build().getCredentials())
          .map(FixedCredentialsProvider.create(_))
    }

  private def makeChannel[F[_]: Sync](config: PubsubFactoryConfig, executor: ScheduledExecutorService): Resource[F, TransportChannel] =
    config.emulatorHost match {
      case Some(emulatorHost) => makeEmulatorChannel(emulatorHost)
      case None               => makeProductionChannel(config.gcpUserAgent, executor)
    }

  /**
   * Makes a `TransportChannel` for when using the pubsub emulator for local development
   */
  private def makeEmulatorChannel[F[_]: Sync](emulatorHost: String): Resource[F, TransportChannel] = {
    val make = Sync[F].delay {
      ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build()
    }
    Resource
      .make(make)(chan => Sync[F].blocking(chan.shutdown()).void)
      .map(GrpcTransportChannel.create(_))
  }

  /**
   * Makes a `TransportChannel` for when _not_ using the pubsub emulator, i.e. the "real" pubsub
   */
  private def makeProductionChannel[F[_]: Sync](
    gcpUserAgent: GcpUserAgent,
    executor: ScheduledExecutorService
  ): Resource[F, TransportChannel] = {
    val makeProvider = Sync[F].delay {
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
        .setExecutor(executor)
        .build
    }

    val make = for {
      provider <- makeProvider
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
