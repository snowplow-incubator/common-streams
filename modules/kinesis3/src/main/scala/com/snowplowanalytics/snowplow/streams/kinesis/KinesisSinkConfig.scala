/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis

import cats.Id
import io.circe._
import io.circe.generic.semiauto._

import java.net.URI

case class KinesisSinkConfigM[M[_]](
  streamName: M[String],
  throttledBackoffPolicy: BackoffPolicy,
  recordLimit: Int,
  byteLimit: Int,
  customEndpoint: Option[URI],
  maxRetries: Int
)

object KinesisSinkConfigM {
  implicit def decoder: Decoder[KinesisSinkConfig] =
    deriveDecoder[KinesisSinkConfig]

  implicit def optionalDecoder: Decoder[Option[KinesisSinkConfig]] =
    deriveDecoder[KinesisSinkConfigM[Option]].map {
      case KinesisSinkConfigM(Some(s), a, b, c, d, e) =>
        Some(KinesisSinkConfigM[Id](s, a, b, c, d, e))
      case _ =>
        None
    }
}
