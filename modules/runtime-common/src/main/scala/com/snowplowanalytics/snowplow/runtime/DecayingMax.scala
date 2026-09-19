/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.{Ref, Sync}
import cats.implicits._

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * The largest duration recorded in the recent past, which expires on its own.
 *
 * This exists because prometheus samples: a scrape sees only an instant. The lag gauge's other
 * term, `currentLag`, is an instantaneous reading, so on its own a batch's latency reaches the
 * published series only if a scrape happens to land while that batch is in flight. Remembering the
 * window maximum is what makes every batch contribute, as the statsd metric's
 * max-over-the-reporting-period does.
 *
 * The keepalive zeros a source records on an empty poll are harmless here, for the same reason they
 * are harmless to statsd: `max(0, x) == x`.
 *
 * `poll` rotates expired intervals out but never clears a live one, so repeated scrapes at the same
 * instant agree.
 *
 * Invariant: never holds a negative maximum.
 *
 * Not micrometer's `io.micrometer.core.instrument.distribution.TimeWindowMax`, despite the same
 * rotating-buffer semantics: that is mutable Java driven by micrometer's own `Clock`, so it cannot
 * be driven by `TestControl`, and the scrape path needs to read this from `F[_]`.
 */
private[runtime] trait DecayingMax[F[_]] {

  /**
   * Record an observation. A negative `value` is floored at zero, which is what upholds the
   * invariant above.
   *
   * Lag is computed as `now - sourceTimestamp`, and for Kafka or Event Hubs that timestamp comes
   * from a producer clock which can run slightly ahead of ours. Note this floor protects only what
   * passes through here: `Metrics` floors the value it publishes separately, because a lag gauge's
   * other term never reaches this class.
   */
  def record(value: FiniteDuration): F[Unit]
  def poll: F[Option[FiniteDuration]]
}

private[runtime] object DecayingMax {

  /**
   * @param newestInterval
   *   index of the interval held at position 0, counted from the monotonic clock's origin
   * @param intervals
   *   maxima, newest first
   */
  private case class State(newestInterval: Long, intervals: Vector[Option[FiniteDuration]])

  /**
   * A recorded value remains visible for at least `window - window / bucketCount` and at most
   * `window`.
   *
   * `window` must be a positive whole number of milliseconds divisible by `bucketCount`; otherwise
   * `intervalOf` divides by zero or the retention guarantee above is not what it says.
   *
   * A bad argument fails the returned `F`, rather than escaping synchronously from the caller's
   * for-comprehension while that `F` is still being constructed. Hence the `defer`, which has to
   * cover the `intervalMillis` division too: that is what divides by a zero `bucketCount`.
   */
  def build[F[_]: Sync](window: FiniteDuration, bucketCount: Int): F[DecayingMax[F]] = Sync[F].defer {
    require(bucketCount > 0, s"DecayingMax bucketCount must be positive, but was $bucketCount")
    require(
      window.toMillis > 0 && window.toMillis % bucketCount == 0,
      s"DecayingMax window ($window) must be a positive whole number of milliseconds divisible by bucketCount ($bucketCount)"
    )

    val intervalMillis = window.toMillis / bucketCount

    // `floorDiv`, not `/`: the monotonic clock's origin is arbitrary and may be negative, and
    // truncation toward zero would make interval 0 two intervals wide there.
    def intervalOf(nowMillis: Long): Long = Math.floorDiv(nowMillis, intervalMillis)

    for {
      // Monotonic, not realTime: this buckets elapsed time, and a backwards NTP step on the wall
      // clock would otherwise freeze `rotate` and merge new records into a stale bucket. The
      // recorded values are durations supplied by the caller, so they are unaffected either way.
      now <- Sync[F].monotonic
      ref <- Ref[F].of(State(intervalOf(now.toMillis), Vector.fill(bucketCount)(none[FiniteDuration])))
    } yield new DecayingMax[F] {

      override def record(value: FiniteDuration): F[Unit] = {
        val floored = value.max(Duration.Zero)
        Sync[F].monotonic.flatMap { now =>
          ref.update { state =>
            val rotated = rotate(state, intervalOf(now.toMillis), bucketCount)
            val merged  = Some(rotated.intervals(0).fold(floored)(_ max floored))
            rotated.copy(intervals = rotated.intervals.updated(0, merged))
          }
        }
      }

      override def poll: F[Option[FiniteDuration]] =
        Sync[F].monotonic.flatMap { now =>
          ref.modify { state =>
            val rotated = rotate(state, intervalOf(now.toMillis), bucketCount)
            (rotated, rotated.intervals.flatten.reduceOption(_ max _))
          }
        }
    }
  }

  private def rotate(
    state: State,
    currentInterval: Long,
    bucketCount: Int
  ): State =
    if (currentInterval <= state.newestInterval)
      state
    else {
      val shift = math.min(currentInterval - state.newestInterval, bucketCount.toLong).toInt
      State(currentInterval, Vector.fill(shift)(none[FiniteDuration]) ++ state.intervals.dropRight(shift))
    }

}
