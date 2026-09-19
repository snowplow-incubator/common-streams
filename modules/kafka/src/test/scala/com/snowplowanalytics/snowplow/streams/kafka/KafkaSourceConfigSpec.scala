/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

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
    Allow a default consumer option to be unset with a null $e3
    Raise an error if the required group id is explicitly unset with a null $e4
    Raise an error on a consumer option which is not a string $e5
    Raise an error on a missing group id for a config which does not inherit our defaults $e6
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
      debounceCommitOffsets = 10.seconds,
      commitTimeout         = 15.seconds
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
      e.show must beEqualTo("DecodingFailure at .xyz.consumerConf: group.id must be set to a non-null value")
    }
  }

  def e3 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |       "group.id": "my-cønsumer-group" # deliberately non-ascii, to cover encodings
    |       "group.instance.id": null
    |       "sasl.mechanism": null
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KafkaSourceConfig(
      topicName        = "my-topic",
      bootstrapServers = "my-bootstrap-server:9092",
      consumerConf = Map(
        "group.id" -> "my-cønsumer-group",
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "latest",
        "security.protocol" -> "SASL_SSL",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      ),
      debounceCommitOffsets = 10.seconds,
      commitTimeout         = 15.seconds
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e4 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |       "group.id": null
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[Wrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .xyz.consumerConf: group.id must be set to a non-null value")
    }
  }

  def e5 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kafka}
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |       "group.id": "my-consumer-group"
    |       "max.poll.records": 500
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[Wrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo(
        "DecodingFailure at .xyz.consumerConf.max.poll.records: Got value '500' with wrong type, expecting string"
      )
    }
  }

  def e6 = {
    val input = """
    |{
    |   "xyz": {
    |     "topicName": "my-topic"
    |     "bootstrapServers": "my-bootstrap-server:9092"
    |     "consumerConf": {
    |       "auto.offset.reset": "latest"
    |     }
    |     "debounceCommitOffsets": "10 seconds"
    |     "commitTimeout": "15 seconds"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[Wrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .xyz.consumerConf: group.id must be set to a non-null value")
    }
  }

}

object KafkaSourceConfigSpec {
  case class Wrapper(xyz: KafkaSourceConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
