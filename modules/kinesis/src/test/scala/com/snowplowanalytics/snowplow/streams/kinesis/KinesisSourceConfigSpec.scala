/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis

import cats.Id
import io.circe.literal._
import com.typesafe.config.ConfigFactory
import io.circe.config.syntax.CirceConfigOps
import io.circe.Decoder
import io.circe.generic.semiauto._
import com.comcast.ip4s.Port
import org.specs2.Specification
import scala.concurrent.duration.DurationLong
import com.snowplowanalytics.snowplow.streams.http.HttpSourceConfigM

class KinesisSourceConfigSpec extends Specification {
  import KinesisSourceConfigSpec._

  def is = s2"""
  The KinesisSource decoder should:
    Decode a valid JSON config with PascalCase type hints $e1
    Decode a valid JSON config with CAPITALIZED type hints $e2
  The KinesisSource defaults should:
    Provide default values from reference.conf with old config format $e3
    Provide default values from reference.conf with new config format $e4
  The KinesisHttpSourceConfig decoder should:
    Decode old config format $e5
    Decode new config format $e6
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
      "leaseDuration": "20 seconds",
      "maxLeasesToStealAtOneTimeFactor": 0.42,
      "checkpointThrottledBackoffPolicy": {
        "minBackoff": "100 millis",
        "maxBackoff": "1second"
      },
      "debounceCheckpoints": "42 seconds",
      "maxRetries": 10,
      "apiCallAttemptTimeout": "42 millis"
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.workerIdentifier must beEqualTo("my-identifier"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.leaseDuration must beEqualTo(20.seconds),
        c.maxLeasesToStealAtOneTimeFactor must beEqualTo(BigDecimal(0.42)),
        c.checkpointThrottledBackoffPolicy must beEqualTo(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second)),
        c.debounceCheckpoints must beEqualTo(42.seconds)
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
      "leaseDuration": "20 seconds",
      "maxLeasesToStealAtOneTimeFactor": 0.42,
      "checkpointThrottledBackoffPolicy": {
        "minBackoff": "100 millis",
        "maxBackoff": "1second"
      },
      "debounceCheckpoints": "42 seconds",
      "maxRetries": 10,
      "apiCallAttemptTimeout": "42 millis"
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.workerIdentifier must beEqualTo("my-identifier"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.leaseDuration must beEqualTo(20.seconds),
        c.maxLeasesToStealAtOneTimeFactor must beEqualTo(BigDecimal(0.42)),
        c.checkpointThrottledBackoffPolicy must beEqualTo(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second)),
        c.debounceCheckpoints must beEqualTo(42.seconds)
      ).reduce(_ and _)
    }
  }

  def e3 = {
    val input = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.aws}
    |   "xyz": {
    |     "appName": "my-app"
    |     "streamName": "my-stream"
    |   }
    |}
    |""".stripMargin

    val result = ConfigFactory.load(ConfigFactory.parseString(input))

    val expected = KinesisSourceConfig(
      appName                          = "my-app",
      streamName                       = "my-stream",
      workerIdentifier                 = System.getenv("HOSTNAME"),
      initialPosition                  = KinesisSourceConfig.InitialPosition.Latest,
      retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750),
      customEndpoint                   = None,
      dynamodbCustomEndpoint           = None,
      cloudwatchCustomEndpoint         = None,
      leaseDuration                    = 10.seconds,
      maxLeasesToStealAtOneTimeFactor  = BigDecimal(2.0),
      checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      debounceCheckpoints              = 10.seconds,
      maxRetries                       = 10,
      apiCallAttemptTimeout            = 15.seconds
    )

    result.as[KinesisSourceConfigWrapper] must beRight.like { case w: KinesisSourceConfigWrapper =>
      w.xyz must beEqualTo(expected)
    }
  }

  def e4 = {
    val input1 = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.aws}
    |   "xyz": {
    |     "kinesis": {
    |       "appName": "my-app"
    |       "streamName": "my-stream"
    |     }
    |   }
    |}
    |""".stripMargin

    val input2 = s"""
    |{
    |   "xyz": $${snowplow.defaults.sources.aws}
    |   "xyz": {
    |     "kinesis": {
    |       "appName": "my-app"
    |       "streamName": "my-stream"
    |     }
    |     "http": {
    |       "port": 8000
    |     }
    |   }
    |}
    |""".stripMargin

    val result1 = ConfigFactory.load(ConfigFactory.parseString(input1))

    val result2 = ConfigFactory.load(ConfigFactory.parseString(input2))

    val expected1 = KinesisHttpSourceConfig(
      kinesis = KinesisSourceConfig(
        appName                          = "my-app",
        streamName                       = "my-stream",
        workerIdentifier                 = System.getenv("HOSTNAME"),
        initialPosition                  = KinesisSourceConfig.InitialPosition.Latest,
        retrievalMode                    = KinesisSourceConfig.Retrieval.Polling(750),
        customEndpoint                   = None,
        dynamodbCustomEndpoint           = None,
        cloudwatchCustomEndpoint         = None,
        leaseDuration                    = 10.seconds,
        maxLeasesToStealAtOneTimeFactor  = BigDecimal(2.0),
        checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
        debounceCheckpoints              = 10.seconds,
        maxRetries                       = 10,
        apiCallAttemptTimeout            = 15.seconds
      ),
      http = None
    )

    val expected2 = expected1.copy(http = Port.fromInt(8000).map(p => HttpSourceConfigM[Id](port = p)))

    val match1 = result1.as[KinesisHttpSourceConfigWrapper] must beRight.like { case w: KinesisHttpSourceConfigWrapper =>
      w.xyz must beEqualTo(expected1)
    }

    val match2 = result2.as[KinesisHttpSourceConfigWrapper] must beRight.like { case w: KinesisHttpSourceConfigWrapper =>
      w.xyz must beEqualTo(expected2)
    }

    match1 and match2
  }

  def e5 = {
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
      "leaseDuration": "20 seconds",
      "maxLeasesToStealAtOneTimeFactor": 0.42,
      "checkpointThrottledBackoffPolicy": {
        "minBackoff": "100 millis",
        "maxBackoff": "1second"
      },
      "debounceCheckpoints": "42 seconds",
      "maxRetries": 10,
      "apiCallAttemptTimeout": "42 millis"
    }
    """

    json.as[KinesisHttpSourceConfig] must beRight.like { case c: KinesisHttpSourceConfig =>
      List(
        c.kinesis.appName must beEqualTo("my-app"),
        c.kinesis.streamName must beEqualTo("my-stream"),
        c.kinesis.workerIdentifier must beEqualTo("my-identifier"),
        c.kinesis.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.kinesis.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.kinesis.leaseDuration must beEqualTo(20.seconds),
        c.kinesis.maxLeasesToStealAtOneTimeFactor must beEqualTo(BigDecimal(0.42)),
        c.kinesis.checkpointThrottledBackoffPolicy must beEqualTo(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second)),
        c.kinesis.debounceCheckpoints must beEqualTo(42.seconds),
        c.http must beNone
      ).reduce(_ and _)
    }
  }

  def e6 = {
    val json = json"""
    {
      "kinesis": {
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
        "leaseDuration": "20 seconds",
        "maxLeasesToStealAtOneTimeFactor": 0.42,
        "checkpointThrottledBackoffPolicy": {
          "minBackoff": "100 millis",
          "maxBackoff": "1second"
        },
        "debounceCheckpoints": "42 seconds",
        "maxRetries": 10,
        "apiCallAttemptTimeout": "42 millis"
      },
      "http": {
        "port": 8000
      }
    }
    """

    json.as[KinesisHttpSourceConfig] must beRight.like { case c: KinesisHttpSourceConfig =>
      List(
        c.kinesis.appName must beEqualTo("my-app"),
        c.kinesis.streamName must beEqualTo("my-stream"),
        c.kinesis.workerIdentifier must beEqualTo("my-identifier"),
        c.kinesis.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.kinesis.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.kinesis.leaseDuration must beEqualTo(20.seconds),
        c.kinesis.maxLeasesToStealAtOneTimeFactor must beEqualTo(BigDecimal(0.42)),
        c.kinesis.checkpointThrottledBackoffPolicy must beEqualTo(BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second)),
        c.kinesis.debounceCheckpoints must beEqualTo(42.seconds),
        c.http.map(_.port) must beEqualTo(Port.fromInt(8000))
      ).reduce(_ and _)
    }
  }

}

object KinesisSourceConfigSpec {
  case class KinesisSourceConfigWrapper(xyz: KinesisSourceConfig)
  case class KinesisHttpSourceConfigWrapper(xyz: KinesisHttpSourceConfig)

  implicit def kinesisSourceConfigWrapperDecoder: Decoder[KinesisSourceConfigWrapper] = deriveDecoder[KinesisSourceConfigWrapper]

  implicit def kinesisHttpSourceConfigWrapperDecoder: Decoder[KinesisHttpSourceConfigWrapper] =
    deriveDecoder[KinesisHttpSourceConfigWrapper]
}
