/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import com.google.api.gax.rpc.FixedHeaderProvider

object GcpUserAgent {
  val solutionUrn: String = "isol_plb32_0014m00002tg62aqad_qufvepzajwfju6jxl5uwg2zn3l3nizte"
  val userAgent: String   = s"Google/ISV Solution (GPN:$solutionUrn)"

  val headerProvider: FixedHeaderProvider =
    FixedHeaderProvider.create("user-agent", userAgent)
}
