/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.http

import com.comcast.ip4s.Port
import io.circe.Decoder

/**
 * Configuration for the HTTP source
 *
 * @param port
 *   The port on which to listen for incoming HTTP requests
 */
case class HttpSourceConfig(
  port: Port
)

object HttpSourceConfig {

  private implicit val portDecoder: Decoder[Port] =
    Decoder.decodeInt.emap(i => Port.fromInt(i).toRight(s"Invalid port: $i"))

  implicit def decoder: Decoder[HttpSourceConfig] =
    Decoder.forProduct1("port")(HttpSourceConfig.apply)
}
