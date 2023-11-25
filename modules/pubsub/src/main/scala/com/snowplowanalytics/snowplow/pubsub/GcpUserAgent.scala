/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.pubsub

import cats.Show
import cats.implicits._
import io.circe.generic.semiauto._
import io.circe.Decoder
import com.google.api.gax.rpc.FixedHeaderProvider

case class GcpUserAgent(productName: String, productVersion: String)

object GcpUserAgent {
  implicit def gcpUserAgentDecoder: Decoder[GcpUserAgent] = deriveDecoder[GcpUserAgent]

  implicit def showGcpUserAgent: Show[GcpUserAgent] = Show { ua =>
    s"${ua.productName}/${ua.productVersion} (GPN:Snowplow;)"
  }

  def headerProvider(ua: GcpUserAgent): FixedHeaderProvider =
    FixedHeaderProvider.create("user-agent", ua.show)
}
