/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq

import io.circe._
import io.circe.generic.semiauto._
import cats.Id

case class NsqSinkConfigM[M[_]](
  topic: M[String],
  nsqdHost: M[String],
  nsqdPort: M[Int],
  byteLimit: Int,
  backoffPolicy: BackoffPolicy
)

object NsqSinkConfigM {
  implicit def decoder: Decoder[NsqSinkConfig] =
    deriveDecoder[NsqSinkConfig]

  implicit def optionalDecoder: Decoder[Option[NsqSinkConfig]] =
    deriveDecoder[NsqSinkConfigM[Option]].map {
      case NsqSinkConfigM(Some(t), Some(h), Some(p), byteLimit, conf) =>
        Some(NsqSinkConfigM[Id](t, h, p, byteLimit, conf))
      case _ =>
        None
    }
}
