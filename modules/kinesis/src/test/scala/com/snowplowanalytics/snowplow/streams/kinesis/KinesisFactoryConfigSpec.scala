/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis

import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

class KinesisFactoryConfigSpec extends Specification {
  import KinesisFactoryConfigSpec._

  def is = s2"""
  The KinesisFactory config should:
    Parse with no user-agent $e1
    Parse with custom user-agent $e2
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": {}
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KinesisFactoryConfig(
      awsUserAgent = None
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e2 = {
    val input = s"""
    |{
    |   "xyz": {
    |     "awsUserAgent": "MyApp/1.0.0"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KinesisFactoryConfig(
      awsUserAgent = Some("MyApp/1.0.0")
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object KinesisFactoryConfigSpec {
  case class Wrapper(xyz: KinesisFactoryConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
