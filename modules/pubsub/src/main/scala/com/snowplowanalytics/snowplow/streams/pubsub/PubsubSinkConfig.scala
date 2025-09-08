/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import cats.Id
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._

import scala.concurrent.duration.FiniteDuration

case class PubsubSinkConfigM[M[_]](
  topic: M[PubsubSinkConfig.Topic],
  batchSize: Int,
  requestByteThreshold: Int,
  retries: PubsubSinkConfig.Retries
)

object PubsubSinkConfig {

  case class TransientErrorRetrying(delay: FiniteDuration, attempts: Int)

  case class Retries(transientErrors: TransientErrorRetrying)

  case class Topic(projectId: String, topicId: String)
}

object PubsubSinkConfigM {
  import PubsubSinkConfig._

  private implicit def topicDecoder: Decoder[Topic] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "topics", topicId) =>
          Right(Topic(projectId, topicId))
        case _ =>
          Left("Expected format: projects/<project>/topics/<topic>")
      }

  implicit def decoder: Decoder[PubsubSinkConfig] = {
    implicit val transientErrorDecoder = deriveDecoder[TransientErrorRetrying]
    implicit val retriesDecoder        = deriveDecoder[Retries]
    deriveDecoder[PubsubSinkConfig]
  }

  implicit def optionalDecoder: Decoder[Option[PubsubSinkConfig]] = {
    implicit val transientErrorDecoder = deriveDecoder[TransientErrorRetrying]
    implicit val retriesDecoder        = deriveDecoder[Retries]
    deriveDecoder[PubsubSinkConfigM[Option]].map {
      case PubsubSinkConfigM(Some(t), a, b, c) =>
        Some(PubsubSinkConfigM[Id](t, a, b, c))
      case _ =>
        None
    }
  }
}
