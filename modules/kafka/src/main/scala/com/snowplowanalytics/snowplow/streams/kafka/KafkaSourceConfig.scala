/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._

import scala.concurrent.duration.FiniteDuration

/**
 * Config to be supplied from the app's hocon
 *
 * @param debounceCommitOffsets
 *   How frequently to commit our progress back to kafka. By increasing this value, we decrease the
 *   number of requests made to the kafka broker.
 */
case class KafkaSourceConfig(
  topicName: String,
  bootstrapServers: String,
  consumerConf: Map[String, String],
  debounceCommitOffsets: FiniteDuration,
  commitTimeout: FiniteDuration
)

object KafkaSourceConfig {
  implicit def decoder: Decoder[KafkaSourceConfig] = deriveDecoder[KafkaSourceConfig]
}
