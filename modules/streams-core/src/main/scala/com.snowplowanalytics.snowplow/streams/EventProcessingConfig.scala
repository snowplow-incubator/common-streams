/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import cats.effect.Sync
import cats.effect.std.Random
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

/**
 * Configures how events are fed into the [[EventProcessor]]
 *
 * @note
 *   This class is not for user-facing configuration. The application itself should have an opinion
 *   on these parameters, e.g. Enrich always wants NoWindowing, but Transformer always wants
 *   Windowing
 *
 * @param windowing
 *   Whether to open a new [[EventProcessor]] to handle a timed window of events (e.g. for the
 *   transformer) or whether to feed events to a single [[EventProcessor]] in a continuous endless
 *   stream (e.g. Enrich)
 *
 * @param latencyConsumer
 *   common-streams apps should use the latencyConsumer to set the latency metric.
 */
case class EventProcessingConfig[F[_]](windowing: EventProcessingConfig.Windowing, latencyConsumer: FiniteDuration => F[Unit])

object EventProcessingConfig {

  sealed trait Windowing
  case object NoWindowing extends Windowing

  /**
   * Configures windows e.g. for Transformer
   *
   * @param duration
   *   The base level duration between windows
   * @param firstWindowScaling
   *   A random factor to adjust the size of the first window. This addresses the situation where
   *   several parallel instances of the app all start at the same time. All instances in the group
   *   should end windows at slightly different times, so that downstream gets a more steady flow of
   *   completed batches.
   * @param numEagerWindows
   *   Controls how many windows are allowed to start eagerly ahead of an earlier window that is
   *   still being finalized. For example, if numEagerWindows=2 then window 42 is allowed to start
   *   while windows 40 and 41 are still finalizing.
   *
   * The `firstWindowScaling` lies in the range 0.25 to 0.5. This range comes from experience with
   * the lake loader: 1. Helps the app quickly reach a stable cpu usage; 2. Avoids a problem in
   * which the loader pulls in more events in the first window than what it can possibly sink within
   * the second window.
   */
  case class TimedWindows(
    duration: FiniteDuration,
    firstWindowScaling: Double,
    numEagerWindows: Int
  ) extends Windowing

  object TimedWindows {
    def build[F[_]: Sync](duration: FiniteDuration, numEagerWindows: Int): F[TimedWindows] =
      for {
        random <- Random.scalaUtilRandom
        factor <- random.betweenDouble(0.25, 0.5)
      } yield TimedWindows(duration, factor, numEagerWindows)
  }

}
