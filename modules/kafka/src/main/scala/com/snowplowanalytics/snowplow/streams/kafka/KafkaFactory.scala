/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import cats.effect.{Async, Ref, Resource, Sync}
import cats.implicits._

import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.kafka.sink.KafkaSink
import com.snowplowanalytics.snowplow.streams.kafka.source.KafkaSource

object KafkaFactory {

  def resource[F[_]: Async]: Resource[F, Factory[F, KafkaSourceConfig, KafkaSinkConfig]] =
    Resource
      .eval(Ref[F].of(AzureAuthenticationCallbackHandler.allClasses))
      .map(impl(_))

  private def impl[F[_]: Async](callbackHandlers: Ref[F, List[String]]): Factory[F, KafkaSourceConfig, KafkaSinkConfig] =
    new Factory[F, KafkaSourceConfig, KafkaSinkConfig] {
      def sink(config: KafkaSinkConfig): Resource[F, Sink[F]] =
        for {
          cbHandler <- Resource.eval(acquireCallbackHandler(callbackHandlers))
          result <- KafkaSink.resource(config, cbHandler)
        } yield result

      def source(config: KafkaSourceConfig): Resource[F, SourceAndAck[F]] =
        for {
          cbHandler <- Resource.eval(acquireCallbackHandler(callbackHandlers))
          result <- KafkaSource.build(config, cbHandler)
        } yield result
    }

  private def acquireCallbackHandler[F[_]: Sync](callbackHandlers: Ref[F, List[String]]): F[String] =
    callbackHandlers
      .modify {
        case Nil    => (Nil, None)
        case h :: t => (t, Some(h))
      }
      .flatMap {
        case Some(ct) =>
          Sync[F].pure(ct)
        case None =>
          Sync[F].raiseError {
            new IllegalStateException("Application exceeded number of pre-dedfined AzureAuthenticationCallbackHandlers")
          }
      }
}
