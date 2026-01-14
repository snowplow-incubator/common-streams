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

import com.snowplowanalytics.snowplow.streams.http.HttpSourceConfig

case class KinesisHttpSourceConfig(
  kinesis: KinesisSourceConfig,
  http: Option[HttpSourceConfig]
)

object KinesisHttpSourceConfig {
  implicit val decoder: Decoder[KinesisHttpSourceConfig] =
    deriveDecoder[KinesisHttpSourceConfig]
      .handleErrorWith(_ => KinesisSourceConfig.decoder.map(KinesisHttpSourceConfig(_, None)))
}
