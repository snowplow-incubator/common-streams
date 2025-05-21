/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kafka

import cats.Id
import cats.implicits._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.DecodingFailure
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

class KafkaSinkConfigSpec extends Specification {
  import KafkaSinkConfigSpec._

  def is = s2"""
  The KafkaSink defaults should:
    Provide default values from reference.conf $e1
    Raise an error on missing required value for client id $e2
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sinks.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "producerConf": {
    |       "client.id": "my-client-id"
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KafkaSinkConfigM[Id](
      topicName        = "my-topic",
      bootstrapServers = "my-bootstrap-server:9092",
      producerConf = Map(
        "client.id" -> "my-client-id",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      )
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e2 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sinks.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "producerConf": {
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[Wrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .xyz.producerConf.client.id: Got value 'null' with wrong type, expecting string")
    }
  }

}

object KafkaSinkConfigSpec {
  case class Wrapper(xyz: KafkaSinkConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
