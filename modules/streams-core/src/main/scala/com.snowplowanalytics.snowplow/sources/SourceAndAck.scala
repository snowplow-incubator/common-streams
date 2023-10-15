/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import fs2.Stream
import scala.concurrent.duration.FiniteDuration

/**
 * The machinery for sourcing events from and external stream and then acking/checkpointing them.
 *
 * Implementations of this trait are provided by the sources library (e.g. kinesis, kafka, pubsub)
 * whereas implementations of [[EventProcessor]] are provided by the specific application (e.g.
 * enrich, transformer, loaders)
 */
trait SourceAndAck[F[_]] {

  /**
   * Wraps the [[EventProcessor]] to create a Stream which, when compiled drained, causes events to
   * flow through the processor.
   *
   * @param config
   *   Configures how events are fed into the processor, e.g. whether to use timed windows
   * @param processor
   *   The EventProcessor, which is implemented by the specific application, e.g. enrich or a loader
   * @return
   *   A stream which should be compiled and drained
   */
  def stream(config: EventProcessingConfig, processor: EventProcessor[F]): Stream[F, Nothing]

  /**
   * Measurement of how long the EventProcessor has spent processing any pending un-acked events.
   *
   * Note, unlike our statsd metrics, this measurement does not consider min/max values over a
   * period of time. It is a snapshot measurement for a single point in time.
   *
   * This measurement is designed to be used as a health probe. If events are getting processed
   * quickly then latency is low and the probe should report healthy. If any event is "stuck" then
   * latency is high and the probe should report unhealthy.
   */
  def processingLatency: F[FiniteDuration]
}
