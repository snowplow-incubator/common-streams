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
import com.snowplowanalytics.snowplow.streams.http.source.HttpSource

class KinesisFactory[F[_]: Async] private (
  client: SdkAsyncHttpClient
) extends Factory[F, KinesisHttpSourceConfig, KinesisSinkConfig] {

  def sink(config: KinesisSinkConfig): Resource[F, Sink[F]] =
    KinesisSink.resource(config, client)

  def source(config: KinesisHttpSourceConfig): Resource[F, SourceAndAck[F]] =
    config.http match {
      case Some(c) => HttpSource.resource(c)
      case None    => Resource.eval(KinesisSource.build(config.kinesis, client))
    }
}

object KinesisFactory {

  def resource[F[_]: Async]: Resource[F, KinesisFactory[F]] =
    makeClient[F].map(new KinesisFactory(_))

  def makeClient[F[_]: Sync]: Resource[F, SdkAsyncHttpClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        NettyNioAsyncHttpClient.builder().build()
      }
    }
}
