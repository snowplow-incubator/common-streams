/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import cats.Id

package object kinesis {
  val AWS_USER_AGENT = "APN/1.1 (ak035lu2m8ge2f9qx90duo3ww)"

  type KinesisSinkConfig = KinesisSinkConfigM[Id]
}
