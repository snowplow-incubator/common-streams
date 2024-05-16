/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.Show
import cats.implicits._
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
   * Reports on whether the source of events is healthy
   *
   * @param maxAllowedProcessingLatency
   *   A maximum allowed value for how long the `EventProcessor` may spend processing any pending
   *   un-acked events. If this cutoff is exceeded then `isHealthy` returns an unhealthy status.
   *
   * Note, unlike our statsd metrics, this latency measurement does not consider min/max values over
   * a period of time. It is a snapshot measurement for a single point in time.
   *
   * If events are getting processed quickly then latency is low and the probe should report
   * healthy. If any event is "stuck" then latency is high and the probe should report unhealthy.
   */
  def isHealthy(maxAllowedProcessingLatency: FiniteDuration): F[SourceAndAck.HealthStatus]
}

object SourceAndAck {

  sealed trait HealthStatus
  case object Healthy extends HealthStatus
  sealed trait Unhealthy extends HealthStatus

  /**
   * The health status expected if the source is at a stage of its lifecycle where cannot provide
   * events
   *
   * For Pubsub this could be because the Subscriber is not yet running. For Kafka this could be due
   * to re-balancing.
   */
  case object Disconnected extends Unhealthy

  /**
   * The health status expected if an event is "stuck" in the EventProcessor
   *
   * @param latency
   *   How long the EventProcessor has spent trying to process the stuck event
   */
  case class LaggingEventProcessor(latency: FiniteDuration) extends Unhealthy

  /**
   * The health status expected if the source of events has been inactive for some time
   *
   * @param duration
   *   How long the source of events has been inactive
   */
  case class InactiveSource(duration: FiniteDuration) extends Unhealthy

  implicit def showUnhealthy: Show[Unhealthy] = Show {
    case Disconnected                   => "No connection to a source of events"
    case LaggingEventProcessor(latency) => show"Processing latency is $latency"
    case InactiveSource(duration)       => show"Source of events has been inactive for $duration"
  }
}
