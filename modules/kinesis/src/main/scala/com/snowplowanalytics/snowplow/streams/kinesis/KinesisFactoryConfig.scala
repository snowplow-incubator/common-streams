/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis

import io.circe._
import io.circe.generic.semiauto._

/**
 * Configuration for the Kinesis factory
 *
 * @param awsUserAgent
 *   Optional custom user-agent string for AWS SDK calls
 */
case class KinesisFactoryConfig(
  awsUserAgent: Option[String]
)

object KinesisFactoryConfig {
  implicit def decoder: Decoder[KinesisFactoryConfig] = deriveDecoder[KinesisFactoryConfig]
}
