/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis

import cats.effect.{Async, Resource, Sync}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.kinesis.sink.KinesisSink
import com.snowplowanalytics.snowplow.streams.kinesis.source.KinesisSource

class KinesisFactory[F[_]: Async] private (
  client: SdkAsyncHttpClient,
  awsUserAgent: Option[String]
) extends Factory[F, KinesisSourceConfig, KinesisSinkConfig] {

  def sink(config: KinesisSinkConfig): Resource[F, Sink[F]] =
    KinesisSink.resource(config, client, awsUserAgent)

  def source(config: KinesisSourceConfig): Resource[F, SourceAndAck[F]] =
    Resource.eval(KinesisSource.build(config, client, awsUserAgent))
}

object KinesisFactory {

  def resource[F[_]: Async](config: KinesisFactoryConfig): Resource[F, KinesisFactory[F]] =
    makeClient[F].map(new KinesisFactory(_, config.awsUserAgent))

  def makeClient[F[_]: Sync]: Resource[F, SdkAsyncHttpClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        NettyNioAsyncHttpClient.builder().build()
      }
    }
}
