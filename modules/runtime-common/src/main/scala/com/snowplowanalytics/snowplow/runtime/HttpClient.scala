/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.{Async, Resource}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import io.circe._
import io.circe.generic.semiauto._

object HttpClient {
  case class Config(
    maxConnectionsPerServer: Int
  )

  object Config {
    implicit def telemetryConfigDecoder: Decoder[Config] = deriveDecoder
  }

  /**
   * Provides a http4s Client configured appropriately for a snowplow common-streams app
   *
   * Blaze option `maxConnectionsPerRequestKey` is set to something small. This is required so we
   * can traverse over many schemas in parallel without overwhelming any single Iglu server.
   *
   * Blaze options `maxTotalConnections` and `maxWaitQueueLimit` are unlimited. This is appropriate
   * because common-streams apps are already naturally rate limited by the flow of events. We do not
   * want exceptions for exceeding number of requests allowed in a queue.
   */
  def resource[F[_]: Async](config: Config): Resource[F, Client[F]] =
    BlazeClientBuilder[F]
      .withMaxConnectionsPerRequestKey(Function.const(config.maxConnectionsPerServer))
      .withMaxTotalConnections(Int.MaxValue)
      .withMaxWaitQueueLimit(Int.MaxValue)
      .resource
}
