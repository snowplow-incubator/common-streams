/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub

import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent

class PubsubSourceConfigSpec extends Specification {
  import PubsubSourceConfigSpec._

  def is = s2"""
  The PubsubSource defaults should:
    Provide default values from reference.conf $e1
  """

  def e1 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.pubsub}
    |   "xyz": {
    |     "subscription": "projects/my-project/subscriptions/my-subscription"
    |     "gcpUserAgent": {
    |       "productVersion": "example-version"
    |     }
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = PubsubSourceConfig(
      subscription            = PubsubSourceConfig.Subscription("my-project", "my-subscription"),
      parallelPullFactor      = BigDecimal(0.5),
      durationPerAckExtension = 1.minute,
      minRemainingAckDeadline = BigDecimal(0.1),
      gcpUserAgent            = GcpUserAgent("Snowplow OSS", "example-version"),
      maxMessagesPerPull      = 1000,
      debounceRequests        = 100.millis
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object PubsubSourceConfigSpec {
  case class Wrapper(xyz: PubsubSourceConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
