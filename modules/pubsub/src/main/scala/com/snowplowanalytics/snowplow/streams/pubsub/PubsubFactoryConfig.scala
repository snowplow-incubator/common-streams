/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import io.circe.Decoder
import io.circe.generic.semiauto._

/**
 * Configures the Pubsub Factory
 *
 * @param gcpUserAgent
 *   configures the user-agent header in requests to pubsub
 * @param emulatorHost
 *   Optional, e.g. `localhost:8181`. Use the pubsub emulator for local development.
 */
case class PubsubFactoryConfig(
  gcpUserAgent: GcpUserAgent,
  emulatorHost: Option[String]
)

object PubsubFactoryConfig {
  implicit def decoder: Decoder[PubsubFactoryConfig] = deriveDecoder[PubsubFactoryConfig]
}
