/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kinesis

import io.circe.literal._
import org.specs2.Specification
import scala.concurrent.duration.FiniteDuration

import java.net.URI

class KinesisSinkConfigSpec extends Specification {

  def is = s2"""
  The KinesisSource decoder should:
    Decode a valid JSON config $e1
  """

  def e1 = {
    val json = json"""
    {
      "streamName": "my-stream",
      "throttledBackoffPolicy": {
        "minBackoff": "100ms",
        "maxBackoff": "500ms",
        "maxRetries": 3
      },
      "recordLimit": 1000,
      "byteLimit": 1000,
      "customEndpoint": "http://localhost:4040"
    }
    """

    json.as[KinesisSinkConfig] must beRight.like { case c: KinesisSinkConfig =>
      List(
        c.streamName must beEqualTo("my-stream"),
        c.throttledBackoffPolicy.minBackoff must beEqualTo(FiniteDuration(100, "ms")),
        c.throttledBackoffPolicy.maxBackoff must beEqualTo(FiniteDuration(500, "ms")),
        c.throttledBackoffPolicy.maxRetries must beEqualTo(Some(3)),
        c.recordLimit must beEqualTo(1000),
        c.byteLimit must beEqualTo(1000),
        c.customEndpoint must beEqualTo(Some(URI.create("http://localhost:4040")))
      ).reduce(_ and _)
    }
  }
}
