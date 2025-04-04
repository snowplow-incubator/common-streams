/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kinesis

import io.circe.literal._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.kinesis.BackoffPolicy

class KinesisSinkConfigSpec extends Specification {
  import KinesisSinkConfigSpec._

  def is = s2"""
  The KinesisSink defaults should:
    Provide default values from reference.conf $e1
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sinks.kinesis}
    |   "xyz": {
    |     "streamName": "my-stream"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KinesisSinkConfigM(
      streamName             = "my-stream",
      throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      recordLimit            = 500,
      byteLimit              = 5242880,
      customEndpoint         = None
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object KinesisSinkConfigSpec {
  case class Wrapper(xyz: KinesisSinkConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
