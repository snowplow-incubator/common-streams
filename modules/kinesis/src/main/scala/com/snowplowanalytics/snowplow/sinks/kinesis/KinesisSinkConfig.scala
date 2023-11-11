/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kinesis

import io.circe._
import io.circe.generic.semiauto._
import io.circe.config.syntax._
import scala.concurrent.duration.FiniteDuration

import java.net.URI

case class BackoffPolicy(
  minBackoff: FiniteDuration,
  maxBackoff: FiniteDuration
)

object BackoffPolicy {

  implicit def backoffPolicyDecoder: Decoder[BackoffPolicy] =
    deriveDecoder[BackoffPolicy]
}

case class KinesisSinkConfig(
  streamName: String,
  throttledBackoffPolicy: BackoffPolicy,
  recordLimit: Int,
  byteLimit: Int,
  customEndpoint: Option[URI]
)

object KinesisSinkConfig {
  implicit def decoder: Decoder[KinesisSinkConfig] =
    deriveDecoder[KinesisSinkConfig]
}
