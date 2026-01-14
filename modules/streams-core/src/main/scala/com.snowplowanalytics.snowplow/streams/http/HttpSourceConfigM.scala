/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.http

import cats.Id
import com.comcast.ip4s.Port
import io.circe._
import io.circe.generic.semiauto._

/**
 * Configuration for the HTTP source
 *
 * @param port
 *   The port on which to listen for incoming HTTP requests
 */
case class HttpSourceConfigM[M[_]](
  port: M[Port]
)

object HttpSourceConfigM {

  private implicit val portDecoder: Decoder[Port] =
    Decoder.decodeInt.emap(i => Port.fromInt(i).toRight(s"Invalid port: $i"))

  implicit def decoder: Decoder[HttpSourceConfig] =
    deriveDecoder[HttpSourceConfig]

  implicit def optionalDecoder: Decoder[Option[HttpSourceConfig]] =
    deriveDecoder[HttpSourceConfigM[Option]].map {
      case HttpSourceConfigM(Some(s)) =>
        Some(HttpSourceConfigM[Id](s))
      case _ =>
        None
    }
}
