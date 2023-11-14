/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.pubsub

import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

class PubsubSinkConfigSpec extends Specification {
  import PubsubSinkConfigSpec._

  def is = s2"""
  The PubsubSink defaults should:
    Provide default values from reference.conf $e1
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sinks.pubsub}
    |   "xyz": {
    |     "topic": "projects/my-project/topics/my-topic"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = PubsubSinkConfig(
      topic                = PubsubSinkConfig.Topic("my-project", "my-topic"),
      batchSize            = 1000L,
      requestByteThreshold = 1000000L
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object PubsubSinkConfigSpec {
  case class Wrapper(xyz: PubsubSinkConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
