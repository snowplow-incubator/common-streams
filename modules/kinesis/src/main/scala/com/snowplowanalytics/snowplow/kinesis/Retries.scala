/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.kinesis

import cats.Applicative
import retry.{RetryPolicies, RetryPolicy}

object Retries {

  /**
   * A retry policy appropriate for when we are throttled by a AWS rate limit.
   *
   * E.g. throttled by Kinesis when sinking records; or throttled by Dynamodb when checkpointing.
   */
  def forThrottling[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
    RetryPolicies.capDelay[F](config.maxBackoff, RetryPolicies.fibonacciBackoff[F](config.minBackoff))
}
