/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq

import cats.effect.{Async, Resource}

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.nsq.sink.NsqSink
import com.snowplowanalytics.snowplow.streams.nsq.source.NsqSource

object NsqFactory {

  def resource[F[_]: Async]: Resource[F, Factory[F, NsqSourceConfig, NsqSinkConfig]] =
    Resource.pure(impl[F])

  private def impl[F[_]: Async]: Factory[F, NsqSourceConfig, NsqSinkConfig] =
    new Factory[F, NsqSourceConfig, NsqSinkConfig] {
      def sink(config: NsqSinkConfig): Resource[F, Sink[F]] =
        NsqSink.resource(config)

      def source(config: NsqSourceConfig): Resource[F, SourceAndAck[F]] =
        Resource.eval(NsqSource.build(config))
    }
}
