/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.effect.kernel.Unique
import fs2.Chunk

import java.nio.ByteBuffer
import java.time.Instant

/**
 * The events as they are fed into a [[EventProcessor]]
 *
 * @param events
 *   Each item in the Chunk is an event read from the external stream, before parsing
 * @param ack
 *   The [[EventProcessor]] must emit this token after it has fully processed the batch of events.
 *   When the [[EventProcessor]] emits the token, it is an instruction to the [[SourceAndAck]] to
 *   ack/checkpoint the events.
 * @param earliestSourceTstamp
 *   The timestamp that an event was originally written to the source stream. Used for calculating
 *   the latency metric.
 */
case class TokenedEvents(
  events: Chunk[ByteBuffer],
  ack: Unique.Token,
  earliestSourceTstamp: Option[Instant]
)
