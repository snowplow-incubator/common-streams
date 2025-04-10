/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kafka

import cats.implicits._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.DecodingFailure
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

class KafkaSourceConfigSpec extends Specification {
  import KafkaSourceConfigSpec._

  def is = s2"""
  The KafkaSource defaults should:
    Provide default values from reference.conf $e1
    Raise an error on missing required value for group id $e2
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |       "group.id": "my-consumer-group"
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KafkaSourceConfig(
      topicName        = "my-topic",
      bootstrapServers = "my-bootstrap-server:9092",
      consumerConf = Map(
        "group.id" -> "my-consumer-group",
        "group.instance.id" -> System.getenv("HOSTNAME"),
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "latest",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      ),
      debounceCommitOffsets = 10.seconds
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e2 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[Wrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .xyz.consumerConf.group.id: Got value 'null' with wrong type, expecting string")
    }
  }

}

object KafkaSourceConfigSpec {
  case class Wrapper(xyz: KafkaSourceConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
