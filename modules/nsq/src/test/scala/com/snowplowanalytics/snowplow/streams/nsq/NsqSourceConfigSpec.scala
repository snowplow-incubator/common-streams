/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq

import com.typesafe.config.ConfigFactory

import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._

import org.specs2.Specification

class NsqSourceConfigSpec extends Specification {
  import NsqSourceConfigSpec._

  def is = s2"""
  The NsqSource defaults should:
    Provide default values from reference.conf $e1
  """

  def e1 = {
    val input = s"""
                   |{
                   |   "xyz": $${snowplow.defaults.sources.nsq}
                   |   "xyz": {
                   |     "topic": "test-topic"
                   |     "channel": "test-channel"
                   |     "lookupHost": "127.0.0.1"
                   |     "lookupPort": 4161
                   |   }
                   |}
                   |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = NsqSourceConfig(
      topic              = "test-topic",
      channel            = "test-channel",
      lookupHost         = "127.0.0.1",
      lookupPort         = 4161,
      maxBufferQueueSize = 3000
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }
}

object NsqSourceConfigSpec {
  case class Wrapper(xyz: NsqSourceConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
