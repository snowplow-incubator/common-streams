/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.nsq

import com.typesafe.config.ConfigFactory

import cats.Id
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._

import scala.concurrent.duration.DurationInt

import org.specs2.Specification

import com.snowplowanalytics.snowplow.nsq.BackoffPolicy

class NsqSinkConfigSpec extends Specification {
  import NsqSinkConfigSpec._

  def is = s2"""
  The NsqSink defaults should:
    Provide default values from reference.conf $e1
  """

  def e1 = {
    val input = s"""
                   |{
                   |   "xyz": $${snowplow.defaults.sinks.nsq}
                   |   "xyz": {
                   |     "topic": "test-topic"
                   |     "nsqdHost": "127.0.0.1"
                   |     "nsqdPort": 4150
                   |   }
                   |}
                   |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = NsqSinkConfigM[Id](
      topic         = "test-topic",
      nsqdHost      = "127.0.0.1",
      nsqdPort      = 4150,
      byteLimit     = 5000000,
      backoffPolicy = BackoffPolicy(minBackoff = 100.milliseconds, maxBackoff = 10.seconds, maxRetries = Some(10))
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object NsqSinkConfigSpec {
  case class Wrapper(xyz: NsqSinkConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
