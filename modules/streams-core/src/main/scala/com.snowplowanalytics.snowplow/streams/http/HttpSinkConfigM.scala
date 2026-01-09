/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.http

import cats.Id
import cats.implicits._

import io.circe._
import io.circe.generic.semiauto._

import org.http4s.{ParseFailure, Uri}

case class HttpSinkConfigM[M[_]](
  baseUri: M[Uri]
)

object HttpSinkConfigM {
  implicit val http4sUriDecoder: Decoder[Uri] =
    Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

  implicit def decoder: Decoder[HttpSinkConfig] =
    deriveDecoder[HttpSinkConfig]

  implicit def optionalDecoder: Decoder[Option[HttpSinkConfig]] =
    deriveDecoder[HttpSinkConfigM[Option]].map {
      case HttpSinkConfigM(Some(s)) =>
        Some(HttpSinkConfigM[Id](s))
      case _ =>
        None
    }
}
