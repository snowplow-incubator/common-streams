/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.nsq

import io.circe._
import io.circe.generic.semiauto._

import com.snowplowanalytics.snowplow.nsq.BackoffPolicy

case class NsqSinkConfig(
  topic: String,
  nsqdHost: String,
  nsqdPort: Int,
  byteLimit: Int,
  backoffPolicy: BackoffPolicy
)

object NsqSinkConfig {
  implicit def decoder: Decoder[NsqSinkConfig] =
    deriveDecoder[NsqSinkConfig]
}
