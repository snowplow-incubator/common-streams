package com.snowplowanalytics.snowplow.kinesis

/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.kinesis

import cats.effect.Sync

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

object Util {
  def getRuntimeRegion[F[_]: Sync]: F[Region] =
    Sync[F].blocking((new DefaultAwsRegionProviderChain).getRegion)
}
