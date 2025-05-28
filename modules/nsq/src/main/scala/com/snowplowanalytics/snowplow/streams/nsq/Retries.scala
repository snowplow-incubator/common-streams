/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq

import cats.Applicative
import retry.{RetryPolicies, RetryPolicy}

private[nsq] object Retries {

  def fullJitter[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] = {
    val capped = RetryPolicies.capDelay[F](config.maxBackoff, RetryPolicies.fullJitter[F](config.minBackoff))
    config.maxRetries.fold(capped)(max => capped.join(RetryPolicies.limitRetries(max)))
  }

}
