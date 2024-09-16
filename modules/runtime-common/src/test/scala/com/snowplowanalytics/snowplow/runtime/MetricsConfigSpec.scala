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
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

class MetricsConfigSpec extends Specification {
  import MetricsConfigSpec._

  def is = s2"""
  The statsd config decoder should:
    Decode a valid JSON config when hostname is set $e1
    Decode a valid JSON config when hostname is missing $e2
    Not decode JSON if other required field is missing $e3
  The statsd defaults should:
    Provide default values from reference.conf $e4
    Not provide default value for prefix $e5


  """

  def e1 = {
    val json = json"""
    {
      "hostname": "statsd.localdomain",
      "port": 5432,
      "tags": {
        "abc": "xyz"
      },
      "period": "42 seconds",
      "prefix": "foo.bar"
    }
    """

    json.as[Option[Metrics.StatsdConfig]] must beRight.like { case Some(c: Metrics.StatsdConfig) =>
      List(
        c.hostname must beEqualTo("statsd.localdomain"),
        c.port must beEqualTo(5432),
        c.tags must beEqualTo(Map("abc" -> "xyz")),
        c.period must beEqualTo(42.seconds),
        c.prefix must beEqualTo("foo.bar")
      ).reduce(_ and _)
    }
  }

  def e2 = {
    val json = json"""
    {
      "port": 5432,
      "tags": {
        "abc": "xyz"
      },
      "period": "42 seconds",
      "prefix": "foo.bar"
    }
    """

    json.as[Option[Metrics.StatsdConfig]] must beRight.like { case c: Option[Metrics.StatsdConfig] =>
      c must beNone
    }
  }

  def e3 = {

    // missing port
    val json = json"""
    {
      "hostname": "statsd.localdomain",
      "tags": {
        "abc": "xyz"
      },
      "period": "42 seconds",
      "prefix": "foo.bar"
    }
    """

    json.as[Option[Metrics.StatsdConfig]] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .port: Missing required field")
    }
  }

  def e4 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.statsd}
    |   "xyz": {
    |     "hostname": "my-statsd-host"
    |     "prefix": "my.custom.prefix"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = Metrics.StatsdConfig(
      hostname = "my-statsd-host",
      port     = 8125,
      tags     = Map.empty,
      period   = 60.seconds,
      prefix   = "my.custom.prefix"
    )

    result.as[StatsdWrapper] must beRight.like { case w: StatsdWrapper =>
      w.xyz must beEqualTo(Some(expected))
    }
  }

  def e5 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.statsd}
    |   "xyz": {
    |     "hostname": "my-statsd-host"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    result.as[StatsdWrapper] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .xyz.prefix: Missing required field")
    }
  }

}

object MetricsConfigSpec {
  case class StatsdWrapper(xyz: Option[Metrics.StatsdConfig])

  implicit def statsdWrapperDecoder: Decoder[StatsdWrapper] = deriveDecoder[StatsdWrapper]
}
