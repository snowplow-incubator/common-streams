/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.nsq

import io.circe._
import io.circe.generic.semiauto._

case class NsqSourceConfig(
  topic: String,
  channel: String,
  lookupHost: String,
  lookupPort: Int,
  maxBufferQueueSize: Int
)

object NsqSourceConfig {
  implicit def decoder: Decoder[NsqSourceConfig] =
    deriveDecoder[NsqSourceConfig]
}
