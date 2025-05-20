/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.implicits._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.DecodingFailure
import io.circe.Decoder
import io.circe.literal._
import io.circe.generic.semiauto._
import org.http4s.implicits._
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

class WebhookConfigSpec extends Specification {
  import WebhookConfigSpec._

  def is = s2"""
  The webhook config decoder should:
    Decode a valid JSON config when endpoint is set $e1
    Decode a valid JSON config when endpoint is missing $e2
    Not decode JSON if other required field is missing $e3
  The webhook defaults should:
    Provide default values from reference.conf $e4
    Not provide default value for endpoint $e5


  """

  def e1 = {
    val json = json"""
    {
      "endpoint": "http://example.com/xyz?abc=123",
      "tags": {
        "abc": "xyz"
      },
      "heartbeat": "42 seconds"
    }
    """

    json.as[Webhook.Config] must beRight { (c: Webhook.Config) =>
      List(
        c.endpoint must beSome(uri"http://example.com/xyz?abc=123"),
        c.tags must beEqualTo(Map("abc" -> "xyz")),
        c.heartbeat must beEqualTo(42.seconds)
      ).reduce(_ and _)
    }
  }

  def e2 = {
    val json = json"""
    {
      "tags": {
        "abc": "xyz"
      },
      "heartbeat": "42 seconds"
    }
    """

    json.as[Webhook.Config] must beRight { (c: Webhook.Config) =>
      List(
        c.endpoint must beNone
      ).reduce(_ and _)
    }
  }

  def e3 = {

    // missing heartbeat
    val json = json"""
    {
      "endpoint": "http://example.com/xyz?abc=123",
      "tags": {
        "abc": "xyz"
      }
    }
    """

    json.as[Webhook.Config] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .heartbeat: Missing required field")
    }
  }

  def e4 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.webhook}
    |   "xyz": {
    |     "endpoint": "http://example.com/xyz?abc=123"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = Webhook.Config(
      endpoint  = Some(uri"http://example.com/xyz?abc=123"),
      tags      = Map.empty,
      heartbeat = 5.minutes
    )

    result.as[ConfigWrapper] must beRight { (w: ConfigWrapper) =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e5 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.webhook}
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[ConfigWrapper] must beRight.like { case w: ConfigWrapper =>
      w.xyz.endpoint must beNone
    }
  }

}

object WebhookConfigSpec {
  case class ConfigWrapper(xyz: Webhook.Config)

  implicit def wrapperDecoder: Decoder[ConfigWrapper] = deriveDecoder[ConfigWrapper]
}
