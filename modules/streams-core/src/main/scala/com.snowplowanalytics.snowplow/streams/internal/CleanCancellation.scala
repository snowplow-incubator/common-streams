/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Async, Deferred, Ref, Sync}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.Queue
import fs2.{Pipe, Stream}

private[internal] object CleanCancellation {
  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /**
   * Consumes one window, so that its batches are processed and acked even when the app is shutting
   * down. Part of the foreground described on `LowLevelSource.sourceAndAckImpl`.
   *
   * The `sinkAndCheckpoint` runs in `concurrently`'s background, so it is not cancelled when this
   * window's stream is cancelled: `concurrently` interrupts it only after the main stream's
   * finalizers have finished.
   *
   * The main stream waits for one of three things:
   *   - The sink finishing, either because the window ended, or because the sink failed (including
   *     finishing early, see `failIfNotEnded`). Nothing more to do.
   *   - Cancellation. In practice, SIGTERM, or `parJoin` cancelling this window because another
   *     window failed. When this window's own sink fails, `concurrently` interrupts the main
   *     stream, which is a cancellation too, unless the main stream sees `sig` complete first and
   *     ends with `Succeeded`. Which of the two happens is a race.
   *   - `stop` completing, because the source raised an error.
   *
   * On cancellation or `stop`, it ends the window by offering `None` to it (see `endWindow`), then
   * waits for the sink to process and ack what it received.
   *
   * @param stop
   *   Completes when the source's error has been recorded. The only thing that ends the window on a
   *   source error.
   * @param startRelease
   *   Schedules the source's `LowLevelSource#releaseUnemitted` and returns once it is scheduled, so
   *   the drain never waits for the release. A release completes if it starts, but it is not
   *   guaranteed to start (see `LowLevelSource.sourceAndAckImpl`). On cancellation only (SIGTERM, a
   *   sibling window failing, or, if it wins the race above, this window's own sink failing), it is
   *   called before the window is ended. With eager windows, every open window calls it, but only
   *   the first release to start calls `releaseUnemitted`.
   */
  def apply[F[_]: Async, A](
    sinkAndCheckpoint: Pipe[F, A, Nothing],
    stop: F[Unit],
    startRelease: F[Unit]
  ): Queue[F, Option[A]] => Stream[F, Nothing] =
    window =>
      Stream.eval((Deferred[F, Unit], Ref[F].of(false)).tupled).flatMap { case (sig, ended) =>
        impl(window, sinkAndCheckpoint, stop, startRelease, sig, ended)
      }

  private def impl[F[_]: Async, A](
    window: Queue[F, Option[A]],
    sinkAndCheckpoint: Pipe[F, A, Nothing],
    stop: F[Unit],
    startRelease: F[Unit],
    sig: Deferred[F, Unit],
    ended: Ref[F, Boolean]
  ): Stream[F, Nothing] =
    Stream
      .eval {
        Async[F].race(sig.get, stop).flatMap {
          case Left(_) =>
            // The sink has completed, either "naturally" because the window ended, or with an error.
            Sync[F].unit
          case Right(_) =>
            Logger[F].warn(
              "The source of events failed. Ending this window if it is still open, and waiting for the event processor to finish it..."
            ) >>
              terminateStream(window, sig)
        }
      }
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          Sync[F].unit
        case ExitCase.Canceled =>
          // SIGTERM, `parJoin` cancelling the windows because one failed, or this window's own sink
          // failing, if `concurrently` interrupts this main stream before it sees `sig` complete (a
          // race: otherwise it ends with `Succeeded` and does not release). This window's sink, if
          // still running, is interrupted only after this finalizer, so it can finish the window.
          Logger[F].warn(
            "This window was cancelled, because the app is shutting down or a window failed. Ending the window if it is still open, and waiting for its event processor to finish..."
          ) >>
            startRelease >>
            terminateStream(window, sig)
        case ExitCase.Errored(_) =>
          // The main stream's own effects failed. A failing sink does not end up here: `concurrently`
          // interrupts the main stream, which then ends with `Canceled` (or with `Succeeded`, if it
          // saw `sig` complete first).
          Logger[F].warn(
            "Ending the current window after an error, if it is still open, and waiting for the event processor to finish it..."
          ) >>
            terminateStream(window, sig).handleErrorWith { e2 =>
              Logger[F].error(e2)("Error when terminating the sink and checkpoint")
            }
      }
      .drain
      .concurrently {
        Stream.bracket(().pure[F])(_ => sig.complete(()).void) >>
          (Stream
            .fromQueueUnterminated(window, 1)
            .evalTap(a => ended.set(true).whenA(a.isEmpty))
            .unNoneTerminate
            .through(sinkAndCheckpoint) ++ Stream.exec(failIfNotEnded(ended)))
            .onFinalizeCase {
              case ExitCase.Succeeded =>
                // The queue has completed "naturally", i.e. a `None` got enqueued.
                Logger[F].info("Completed sinking and checkpointing events")
              case ExitCase.Canceled =>
                Logger[F].info("Sinking and checkpointing was cancelled")
              case ExitCase.Errored(e) =>
                Logger[F].error(e)("Error on sinking and checkpointing events")
            }
      }

  // A sink that completes before it has taken its window's `None` has abandoned the window: the
  // source's pipeline would block forever offering it the next batch. It is turned into an error,
  // which then takes the same path as an error raised by the event processor. No later window can
  // exist to ack past the abandoned batches (see the window-opening invariant on
  // `LowLevelSource.sourceAndAckImpl`).
  private def failIfNotEnded[F[_]: Sync](ended: Ref[F, Boolean]): F[Unit] =
    ended.get.flatMap {
      case true  => Sync[F].unit
      case false => Sync[F].raiseError(new IllegalStateException(ProcessorFinishedEarly))
    }

  val ProcessorFinishedEarly = "The event processor finished before the end of its window"

  // Ends the window by offering `None`. The sink can still take batches that the source offers
  // before the forked fiber's `None` is in place; they are processed and acked like the rest.
  // Forked because, on a synchronous queue, the offer blocks until the sink takes it, and
  // the sink never does if the window already has its `None` (it ended by itself, or by the source's
  // error). The fiber then stays parked, harmlessly, until the app exits.
  private def endWindow[F[_]: Async, A](queue: Queue[F, Option[A]]): F[Unit] =
    queue.offer(None).start.void

  private def terminateStream[F[_]: Async, A](queue: Queue[F, Option[A]], sig: Deferred[F, Unit]): F[Unit] =
    for {
      _ <- endWindow(queue)
      _ <- sig.get
    } yield ()
}
