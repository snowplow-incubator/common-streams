/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import fs2.Chunk

import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration

/**
 * The events and checkpointable item emitted by a LowLevelSource
 *
 * This library uses LowLevelEvents internally, but it is never exposed to the high level event
 * processor
 *
 * @param earliestSourceTstamp
 *   A point in time, represented as a `FiniteDuration` since epoch
 */
private[streams] case class LowLevelEvents[C](
  events: Chunk[ByteBuffer],
  ack: C,
  earliestSourceTstamp: Option[FiniteDuration]
)
