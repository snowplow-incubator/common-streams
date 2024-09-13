/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import io.circe.literal._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification
import scala.concurrent.duration.DurationLong

class KinesisSourceConfigSpec extends Specification {
  import KinesisSourceConfigSpec._

  def is = s2"""
  The KinesisSource decoder should:
    Decode a valid JSON config with PascalCase type hints $e1
    Decode a valid JSON config with CAPITALIZED type hints $e2
  The KinesisSource defaults should:
    Provide default values from reference.conf $e3
  """

  def e1 = {
    val json = json"""
    {
      "appName": "my-app",
      "streamName": "my-stream",
      "workerIdentifier": "my-identifier",
      "retrievalMode": {
        "type": "Polling",
        "maxRecords": 42
      },
      "initialPosition": {
        "type": "TrimHorizon"
      },
      "leaseDuration": "20 seconds"
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.workerIdentifier must beEqualTo("my-identifier"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.leaseDuration must beEqualTo(20.seconds)
      ).reduce(_ and _)
    }
  }

  def e2 = {
    val json = json"""
    {
      "appName": "my-app",
      "streamName": "my-stream",
      "workerIdentifier": "my-identifier",
      "retrievalMode": {
        "type": "POLLING",
        "maxRecords": 42
      },
      "initialPosition": {
        "type": "TRIM_HORIZON"
      },
      "leaseDuration": "20 seconds"
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.workerIdentifier must beEqualTo("my-identifier"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.leaseDuration must beEqualTo(20.seconds)
      ).reduce(_ and _)
    }
  }

  def e3 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.kinesis}
    |   "xyz": {
    |     "appName": "my-app"
    |     "streamName": "my-stream"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KinesisSourceConfig(
      appName                  = "my-app",
      streamName               = "my-stream",
      workerIdentifier         = System.getenv("HOSTNAME"),
      initialPosition          = KinesisSourceConfig.InitialPosition.Latest,
      retrievalMode            = KinesisSourceConfig.Retrieval.Polling(1000),
      customEndpoint           = None,
      dynamodbCustomEndpoint   = None,
      cloudwatchCustomEndpoint = None,
      leaseDuration            = 10.seconds
    )

    result.as[Wrapper] must beRight.like { case w: Wrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

}

object KinesisSourceConfigSpec {
  case class Wrapper(xyz: KinesisSourceConfig)

  implicit def wrapperDecoder: Decoder[Wrapper] = deriveDecoder[Wrapper]
}
