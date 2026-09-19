/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import cats.Id
import io.circe.Decoder
import io.circe.generic.semiauto._

case class KafkaSinkConfigM[M[_]](
  topicName: M[String],
  bootstrapServers: M[String],
  producerConf: Map[String, String]
)

object KafkaSinkConfigM {
  implicit def decoder: Decoder[KafkaSinkConfig] = {
    implicit val producerConfDecoder: Decoder[Map[String, String]] =
      KafkaConfigDecoders.clientConf("client.id")
    deriveDecoder[KafkaSinkConfig]
  }

  implicit def optionalDecoder: Decoder[Option[KafkaSinkConfig]] = {
    implicit val producerConfDecoder: Decoder[Map[String, String]] =
      KafkaConfigDecoders.clientConf("client.id")
    deriveDecoder[KafkaSinkConfigM[Option]].map {
      case KafkaSinkConfigM(Some(t), Some(b), conf) =>
        Some(KafkaSinkConfigM[Id](t, b, conf))
      case _ =>
        None
    }
  }

}
