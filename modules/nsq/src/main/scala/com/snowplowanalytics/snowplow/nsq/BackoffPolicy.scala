/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.nsq

import io.circe._
import io.circe.config.syntax._
import io.circe.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class BackoffPolicy(
  minBackoff: FiniteDuration,
  maxBackoff: FiniteDuration,
  maxRetries: Option[Int]
)

object BackoffPolicy {
  implicit def decoder: Decoder[BackoffPolicy] =
    deriveDecoder[BackoffPolicy]
}
