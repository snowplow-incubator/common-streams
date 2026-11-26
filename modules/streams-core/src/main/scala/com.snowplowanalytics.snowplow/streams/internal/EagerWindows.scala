/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.implicits._
import cats.effect.Concurrent
import cats.effect.kernel.Deferred
import fs2.Stream

private object EagerWindows {

  sealed trait EagerWindowResult
  case object PreviousWindowFailed extends EagerWindowResult
  case object PreviousWindowSuccess extends EagerWindowResult

  /**
   * Raised by a window, at its first ack or at the end of its sink if it acks nothing, when the
   * window before it failed. Left out of the stream's final error by
   * `LowLevelSource.withoutAbandonedWindows`.
   */
  final class AbandonedWindowException extends RuntimeException("Eager window abandoned because previous window failed")

  /** One window's link to the window before it and the window after it */
  trait Control[F[_]] {

    /**
     * Called once, when this window has finished, successfully or not. Passes on the result as
     * described on [[withControls]], without waiting.
     */
    def unblockNextWindow(result: EagerWindowResult): F[Unit]

    /**
     * Called before each ack, and once at the end of the window's sink. Waits until the previous
     * window has finished, then fails if that window failed, so this window never acks, or finishes
     * successfully, after its predecessor failed.
     */
    def waitForPreviousWindow: F[Unit]
  }

  /**
   * This is the machinery which allows us to run multiple windows in parallel (slightly
   * overlapping)
   *
   * The second window is allowed to start processing before the first window has finished. This is
   * helpful for maximizing usage of the available CPU.
   *
   * But the second window is forbidden from checkpointing until the previous window finishes with
   * success. This prevents the second window from checkpointing earlier events which are not yet
   * safely committed to the output. If the previous window failed, the second window fails at its
   * first checkpoint, so it is abandoned without acking.
   *
   * A window finishes successfully only after its predecessor has: `LowLevelSource.messageSink`
   * calls `waitForPreviousWindow` at the end of the window's sink, whether or not the window acked.
   * So passing on success is always right, and a window acks only after every earlier window has
   * finished successfully, and never after any earlier window failed. A window that fails or is
   * cancelled passes on failure.
   *
   * The chain runs across inner-stream boundaries. A window can ack nothing. For example,
   * `timedWindows` opens the first window of each inner stream before any data arrives, so that
   * window is empty when the inner stream ends, or the window times out, before any data arrives: a
   * quiet spell after a rebalance or a reshard. A window that acked already waited for its
   * predecessor, so the wait at the end of its sink returns at once. A window that acked nothing
   * waits there.
   *
   * This is a deliberate trade-off: while an empty window waits, it holds its `parJoin` slot, so
   * one eager slot is lost. With `numEagerWindows = 1`, a window opened while the empty window
   * waits can't start until the predecessor has finished, and the source back-pressures meanwhile.
   * The wait is part of the sink, so on SIGTERM `CleanCancellation` waits for it too. It ends when
   * the predecessor finishes, because a window's predecessor's `messageSink` finalizer always runs
   * and completes the `Deferred` the window waits on. That follows from the window-opening
   * invariant on `LowLevelSource.sourceAndAckImpl`.
   *
   * Emits one control per window, in window order.
   */
  def withControls[F[_]: Concurrent, A](f: Control[F] => A): Stream[F, A] =
    controls.map(f)

  private def controls[F[_]: Concurrent]: Stream[F, Control[F]] =
    Stream
      .eval(Deferred[F, EagerWindowResult])
      .repeat
      .zipWithPrevious
      .map {
        case (Some(previous), next) =>
          new Control[F] {
            def unblockNextWindow(result: EagerWindowResult): F[Unit] =
              next.complete(result).void
            def waitForPreviousWindow: F[Unit] =
              previous.get.flatMap {
                case PreviousWindowFailed =>
                  Concurrent[F].raiseError(new AbandonedWindowException)
                case PreviousWindowSuccess =>
                  Concurrent[F].unit
              }
          }
        case (None, next) =>
          new Control[F] {
            def unblockNextWindow(result: EagerWindowResult): F[Unit] =
              next.complete(result).void
            def waitForPreviousWindow: F[Unit] =
              Concurrent[F].unit
          }
      }

}
