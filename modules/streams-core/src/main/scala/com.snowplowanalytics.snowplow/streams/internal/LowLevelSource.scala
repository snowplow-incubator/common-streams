/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.{Monad, Semigroup}
import cats.implicits._
import cats.effect.implicits._
import cats.effect.std.{Queue, Supervisor}
import cats.effect.kernel.{Deferred, Ref, Unique}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.{Async, Sync}
import fs2.{CompositeFailure, Pipe, Pull, Stream}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}

/**
 * A common interface over external sources of events
 *
 * This library uses [[LowLevelSource]] internally as a stepping stone towards implementing a
 * [[SourceAndAck]].
 *
 * Terms used throughout this file, [[CleanCancellation]], [[EagerWindows]] and [[Checkpointer]]:
 *
 *   - The source's '''outer stream''' is the stream returned by [[LowLevelSource#stream]]. Its
 *     '''inner streams''' are the streams of batches it emits. "Inner stream" means nothing else.
 *   - A '''window''' is a run of batches from a single inner stream, handed to one invocation of
 *     the [[EventProcessor]]. With `NoWindowing` there is one window per inner stream. With
 *     `TimedWindows` an inner stream is cut into windows of a fixed duration.
 *   - A window '''ends''' when it receives its last batch, and '''finishes''' when the event
 *     processor has processed it and its batches are acked (or, after a failure, abandoned).
 *   - The source's streams are '''finalized''' when their fs2 finalizers run, i.e. when the
 *     source's resources are released.
 *
 * @tparam F
 *   An IO effect type
 * @tparam C
 *   A source-specific thing which can be checkpointed
 */
private[streams] trait LowLevelSource[F[_], C] {

  def checkpointer: Checkpointer[F, C]

  /**
   * Provides a stream of stream of low level events
   *
   * The inner streams are consumed one after another. With `TimedWindows` and `numEagerWindows >=
   * 1`, the first window of the next inner stream can start while the last window of the previous
   * one is still finishing (see [[EagerWindows]]).
   *
   * A new inner stream is emitted when the source requires a hard boundary, e.g. for Kafka when a
   * client rebalance occurs, or for Kinesis when a shard ends.
   *
   * The [[EventProcessor]] is invoked once per window. A window never spans two inner streams.
   *
   * An inner stream must emit `None` frequently, or the `SourceAndAck` will report itself as
   * unhealthy. But a `None` should mean specifically "a fetch from upstream succeeded and returned
   * nothing".
   *
   * `monitorLatency` reports a latency of zero for every `None`, which is a true measurement only
   * under that stronger contract: the source really did ask upstream, and there really was nothing
   * waiting. It follows that the *absence* of reported latencies means the source itself has
   * stalled, which is what the production alert on missing metrics detects. A source that emits
   * `None` on a timer keeps the health check satisfied, but gives up that second guarantee
   * silently.
   *
   * Kinesis satisfies the stronger contract via KCL's `callProcessRecordsEvenForEmptyRecordList`,
   * and PubSub via emitting `None` whenever a pull is established or returns no messages, in both
   * its streaming and unary modes.
   *
   * Kafka, NSQ and Http do not: each merges a 10-second keepalive that emits `None` on a timer,
   * because none of their underlying clients exposes an equivalent signal. So neither the health
   * check nor the alert on missing metrics detects a stalled source for those three.
   *
   * '''When the source's streams are finalized'''
   *
   * On cancellation, on an error raised by the event processor, and on an error raised by the
   * source, the source's streams are finalized only after the open windows have finished and the
   * checkpointer has been used for the last time: a final `nack` of the batches the event processor
   * never reported as processed. Until then the source keeps running, and any batches it emits
   * meanwhile are nacked at the end. The exceptions:
   *
   *   - A stream that raises an error is finalized by the error itself, and emits nothing more. For
   *     an error raised in an inner stream, the outer stream and its resources stay alive until the
   *     final nack. An error raised in the outer stream finalizes the outer stream and the running
   *     inner stream at once, and the open windows then finish without them.
   *   - An inner stream that ends by itself is finalized as soon as it ends, usually before its
   *     last window has finished. The generic layer only learns that an inner stream has ended by
   *     pulling from it, by which time its finalizers have run. A source that must wait until its
   *     last batch is acked does so through its checkpointable, as the Kinesis source does for a
   *     shard end.
   *   - An outer stream that ends by itself is likewise finalized as soon as it ends, possibly
   *     before the windows of its last inner stream have finished.
   *
   * This applies to both `NoWindowing` and `TimedWindows`.
   *
   * '''Batches the source is responsible for'''
   *
   * A batch that is neither acked nor nacked is never committed, so nothing is lost, but the
   * source's own finalizers must account for it. That happens to:
   *   - batches the event processor had processed in a window that failed, or was abandoned after
   *     an earlier window failed, before they were acked;
   *   - a batch the source emits after the final nack has read the outstanding batches, and before
   *     its streams are interrupted;
   *   - a batch in flight when the outer stream raised an error, between the source emitting it and
   *     the generic layer recording it for acking.
   *
   * '''Guidance for source implementations'''
   *
   *   - Keep long-lived resources, such as clients and background work like ack-deadline extension,
   *     in the outer stream or outside the stream altogether. Only those are guaranteed to outlive
   *     the windows on an inner-stream error.
   *   - Raise errors from the inner streams, not the outer stream, so that the outer stream's
   *     resources remain available while the open windows finish.
   *   - To wait until a batch is acked, wait on something the checkpointable carries and the
   *     [[Checkpointer]] releases on ack, as the Kinesis source does with a `CountDownLatch`. Don't
   *     rely on a finalizer running after the ack, because of the two "ends by itself" clauses
   *     above.
   */
  def stream: Stream[F, Stream[F, Option[LowLevelEvents[C]]]]

  /**
   * How frequently we should checkpoint progress to this source
   *
   * E.g. for the Kinesis we can increase value to reduce how often we need to write to the DynamoDB
   * table
   */
  def debounceCheckpoints: FiniteDuration

  /**
   * Gives back to the upstream every batch this source has pulled but not yet emitted downstream,
   * and emits no batch afterwards. A batch that reaches the source afterwards, from a pull already
   * in flight or a later one, is given back too.
   *
   * [[CleanCancellation]] schedules it when a window is cancelled: on SIGTERM, and when `parJoin`
   * cancels the other windows after one window failed. The failing window itself schedules it only
   * if `concurrently` interrupts it before its sink has finished, which is a race, so with
   * `NoWindowing` or no eager windows a processor failure often does not release early. It is
   * scheduled before the window is ended, so the released batches can be redelivered elsewhere
   * while the event processor finishes its window, without waiting for the end of shutdown. Batches
   * already emitted are unaffected: those the event processor reports as processed are acked
   * through the [[Checkpointer]], and those still waiting in the generic windowing's buffers are
   * nacked by the source at the end of shutdown, as usual.
   *
   * It runs as an uncancelable effect in a `Supervisor` scoped to one run of the stream (see
   * `sourceAndAckImpl`), alongside the drain: the drain does not wait for it, but the stream does
   * not end until it completes. Its errors are logged there.
   *
   * Contract for implementations:
   *   - It is best-effort. Shutdown must stay correct if it is never called or raises an error.
   *   - It must be bounded, because it cannot be cancelled and the stream waits for it.
   *   - It is called at most once per run of the stream.
   *   - A batch must never be both released by it and emitted downstream.
   *   - The source must not emit `None` in place of a released batch, because `None` means "a fetch
   *     from upstream returned nothing" (see [[stream]]).
   *
   * A source with nothing to gain from an early release implements it as a no-op.
   */
  def releaseUnemitted: F[Unit]
}

private[streams] object LowLevelSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /**
   * Mutable state held by the SourceAndAck implementation
   *
   * Map is keyed by Token, corresponding to a batch of `TokenedEvents`. Map values are `C`s which
   * is how to ack/checkpoint the batch.
   */
  private type AcksState[C] = Map[Unique.Token, C]

  /**
   * A window of events, handed from the source's pipeline to an [[EventProcessor]]
   *
   * The window ends when `None` is enqueued.
   */
  private type Window[F[_]] = Queue[F, Option[TokenedEvents]]

  private sealed trait InternalState

  private object InternalState {

    /**
     * The Source is awaiting the downstream processor to pull another message from us
     *
     * @param since
     *   Timestamp of when the last message was emitted downstream. A point in time represented as
     *   FiniteDuration since the epoch.
     * @param streamTstamp
     *   For the last last batch to be emitted downstream, this is the earliest timestamp according
     *   to the source, i.e. time the event was written to the source stream. It is None if this
     *   stream type does not record timestamps, e.g. Kafka under some circumstances.
     */
    case class AwaitingDownstream(since: FiniteDuration, streamTstamp: Option[FiniteDuration]) extends InternalState

    /**
     * The Source is awaiting the upstream remote source to provide more messages
     *
     * @param since
     *   Timestamp of when the last message was received up upstream. A point in time represented as
     *   FiniteDuration since the epoch.
     */
    case class AwaitingUpstream(since: FiniteDuration) extends InternalState

    case object Disconnected extends InternalState
  }

  /**
   * Lifts the internal [[LowLevelSource]] into a [[SourceAndAck]], which is the public API of this
   * library
   */
  def toSourceAndAck[F[_]: Async, C](source: LowLevelSource[F, C]): F[SourceAndAck[F]] =
    for {
      stateRef <- Ref[F].of[InternalState](InternalState.Disconnected)
      acksRef <- Ref[F].of(Map.empty[Unique.Token, C])
    } yield sourceAndAckImpl(source, stateRef, acksRef)

  /**
   * '''Design'''
   *
   * The stream is split into two halves, run as `foreground.concurrently(background)`:
   *
   *   - The '''background''' ([[sourceWindows]]) owns the source: its outer and inner streams, the
   *     health-state bracket, `monitorLatency`, `tokened` and windowing. It fills every window
   *     itself.
   *   - The '''foreground''' owns the windows' sinks: for each window, a [[CleanCancellation]]
   *     around `messageSink`, gated by an [[EagerWindows]] control. It runs them under `parJoin`,
   *     so that eager windows overlap, and it makes the final nack.
   *
   * Two kinds of synchronous queue cross between them: the `windows` queue, which hands each new
   * window to the foreground, and each window's own queue, which carries its batches and is ended
   * by `None`.
   *
   * '''Shutdown paths'''
   *
   * With `NoWindowing`, or `numEagerWindows = 0`, the eagerness is 1, and `parJoin(1)` is
   * `outer.flatten`: only one window is ever open, so the steps below that concern other windows,
   * and the `Scope.lease` invariant, apply only when `numEagerWindows >= 1`.
   *
   * Cancellation (in practice, SIGTERM):
   *   1. The foreground stops taking new windows, and each open window's stream is cancelled.
   *   1. [[CleanCancellation]] schedules the source's `releaseUnemitted` under the supervisor,
   *      without waiting for it. It then ends each window that is still open by offering it `None`,
   *      and waits for its sink to process and ack what it received.
   *   1. Once every window has finished, `nackUnhandled` nacks the batches that were never reported
   *      as processed.
   *   1. `concurrently` interrupts the background, which finalizes the source's streams.
   *   1. The supervisor waits for every release that has begun to complete.
   *
   * An error raised by the event processor (or a sink that finishes early, see
   * [[CleanCancellation]]'s `failIfNotEnded`):
   *   1. `messageSink` fails, and unblocks the next eager window with `PreviousWindowFailed`, so
   *      that window, and every later one, fails at its first ack, or at the end of its sink if it
   *      acks nothing (see [[EagerWindows]]).
   *   1. With `numEagerWindows >= 1`, `parJoin` cancels the other windows. Each ends and finishes
   *      as on cancellation, including scheduling the source's `releaseUnemitted`. The failing
   *      window schedules it too only if `concurrently` interrupts it before its sink has finished
   *      (a race).
   *   1. `nackUnhandled`, then the background is interrupted, then the supervisor waits for the
   *      releases, as on cancellation.
   *   1. The stream fails with the processor's error, as [[withoutAbandonedWindows]] describes.
   *
   * An error raised by the source:
   *   1. In an inner stream: the error passes through windowing and finalizes the inner stream. The
   *      per-inner-stream `handleErrorWith` in [[sourceWindows]] records it in `sourceError` and
   *      holds the outer stream open.
   *   1. In the outer stream: the error interrupts the running inner stream, if any. Both streams
   *      are finalized. The outer `handleErrorWith` records the error.
   *   1. Once the error is recorded, the foreground stops taking new windows (`interruptWhen`), and
   *      [[CleanCancellation]] ends each window that is still open (its `stop`). Nothing else ends
   *      a window on a source error: windowing never offers `None` on an error, so recording the
   *      error never waits on a window's consumer, which may already have gone.
   *   1. `nackUnhandled`, then the background is interrupted, which finalizes the outer stream if
   *      it is still open.
   *   1. The stream fails with the source's error, whatever the foreground raised meanwhile.
   *
   * '''The release supervisor'''
   *
   * The `Supervisor` that runs `releaseUnemitted` is the outermost resource of `stream`, so it is
   * finalized after everything else in one run of the stream. Apps run the stream inside the
   * source's resource, so every release completes before the source's own resources are released.
   *
   * However many windows schedule the release, `releaseUnemitted` is called at most once per run:
   * the first release to start calls it, and later ones do nothing.
   *
   * The release is `uncancelable` because of how the supervisor finalizes: when the stream exits
   * with `Canceled` or `Errored` (SIGTERM exits with `Canceled`), the supervisor cancels its fibers
   * rather than waiting for them, and cancelling an uncancelable fiber waits for it to complete. So
   * a release completes once it has started. It is not guaranteed to run: a cancel can land before
   * the supervised fiber starts, for example when an event processor fails and the cancelled
   * windows finish at once. That is harmless, because the source nacks those batches at the end of
   * shutdown.
   *
   * '''Invariants'''
   *
   * Anyone changing this code must keep these. Each is guarded by a test in `LowLevelSourceSpec`,
   * unless named otherwise.
   *
   *   - The source's pipeline runs in `concurrently`'s background, never as `parJoin`'s outer
   *     stream. With `numEagerWindows >= 1`, `parJoin` defers its outer stream's finalizers using
   *     `Scope.lease`, which reaches only direct child scopes, so a source's finalizers nested any
   *     deeper would run as soon as cancellation starts. `concurrently` interrupts its background
   *     only after the foreground, and all of its finalizers, have finished. Guarded by
   *     `finalize1`, `finalize2` and `finalize3`.
   *   - The source's errors are caught per inner stream, before `flatten`. An error that reached
   *     the outer stream would finalize the outer stream's resources (for example KCL's scheduler)
   *     before the open windows are acked. Guarded by `finalize4` and `finalize5`.
   *   - `nackUnhandled` runs as a finalizer of the foreground, inside `concurrently`, so the
   *     checkpointer is never used after the source's streams are finalized by cancellation or
   *     interruption. Guarded by `finalize1` and `finalize7`.
   *   - A window's consumer never drives the source. The background evaluates each window's input
   *     itself (`singleWindow`, `timedWindows`), and fills the window from its own pull, blocking
   *     on the window's synchronous queue until the consumer takes each batch. Otherwise the
   *     health-state bracket would close too early, and windows would stall when the foreground
   *     takes them one at a time (`numEagerWindows = 0`). Guarded by `health2`, `health7`, and
   *     `windowed7` in `LowLevelSourceNoEagerWindowsSpec`.
   *   - An event crosses exactly one synchronous queue between the source and the sink: the
   *     window's own queue. Every extra hop is another batch in flight. Guarded by `windowed4`.
   *   - The source's original error takes precedence over any error raised while the windows
   *     finish. Guarded by `finalize8`.
   *   - A source error is recorded without waiting on any window. Guarded by
   *     `sourceErrorAfterSinkGone`, in `LowLevelSourceErrorPathsSpec`.
   *   - A window's sink never completes silently before its window has ended. Guarded by
   *     `abandoned1` and `abandoned2`, in `LowLevelSourceErrorPathsSpec`.
   *   - An abandoned window's error never replaces the error that caused it. Guarded by
   *     `abandonedFirst`, in `LowLevelSourceErrorPathsSpec`.
   *   - The pipeline opens a window only after its predecessor's sink has taken the predecessor's
   *     `None`: `singleWindow` and `timedWindows` offer `None` on the window's synchronous queue,
   *     and wait for it to be taken, before they emit the next window. So once a window exists, its
   *     predecessor's `messageSink` has started, and its finalizer always runs. The chain in
   *     [[EagerWindows]] and `failIfNotEnded` in [[CleanCancellation]] rely on it. No test guards
   *     it: no test in `streams-core` fails when the `None` is offered without waiting for it to be
   *     taken.
   */
  private def sourceAndAckImpl[F[_]: Async, C](
    source: LowLevelSource[F, C],
    stateRef: Ref[F, InternalState],
    acksRef: Ref[F, Map[Unique.Token, C]]
  ): SourceAndAck[F] = new SourceAndAck[F] {
    def stream(config: EventProcessingConfig[F], processor: EventProcessor[F]): Stream[F, Nothing] =
      (for {
        supervisor <- Stream.resource(Supervisor[F](await = true))
        releaseStarted <- Stream.eval(Ref[F].of(false))
        windows <- Stream.eval(Queue.synchronous[F, Option[Window[F]]])
        sourceError <- Stream.eval(Deferred[F, Throwable])
      } yield run(config, processor, startRelease(supervisor, releaseStarted, source.releaseUnemitted), windows, sourceError)).flatten

    private def run(
      config: EventProcessingConfig[F],
      processor: EventProcessor[F],
      release: F[Unit],
      windows: Queue[F, Option[Window[F]]],
      sourceError: Deferred[F, Throwable]
    ): Stream[F, Nothing] = {
      val stopOnSourceError: F[Unit] = sourceError.get.void

      val sinks = EagerWindows.withControls { control: EagerWindows.Control[F] =>
        CleanCancellation(
          messageSink(processor, acksRef, source.checkpointer, control, source.debounceCheckpoints),
          stopOnSourceError,
          release
        )
      }

      val foreground = Stream
        .fromQueueNoneTerminated(windows)
        // On a source error, stop taking new windows. This interrupts only `parJoin`'s outer
        // stream: the open windows go on, and are ended by `CleanCancellation`'s `stop`, which
        // completes on the same signal. Nothing else gives an open window its `None` on a source
        // error.
        .interruptWhen(stopOnSourceError.map(_.asRight[Throwable]))
        .zip(sinks)
        .map { case (window, sink) => sink(window) }
        .parJoin(eagerness(config.windowing)) // so we start processing the next window while the previous window is still finishing.
        // Must stay a finalizer of the foreground, inside `concurrently` (see Invariants).
        .onFinalize {
          nackUnhandled(source.checkpointer, acksRef)
        }

      val background = sourceWindows(config, windows, sourceError)

      // The source's error takes precedence: `handleErrorWith` replaces whatever the foreground
      // raised (for example a failed checkpoint, already logged by `CleanCancellation`), and
      // `append` raises it when the foreground finished cleanly after being stopped by it.
      // Otherwise the foreground's error is raised without its abandoned windows.
      foreground
        .concurrently(background)
        .handleErrorWith { e =>
          Stream.eval(sourceError.tryGet).flatMap(original => Stream.raiseError[F](original.getOrElse(withoutAbandonedWindows(e))))
        }
        .append {
          Stream.eval(sourceError.tryGet).flatMap {
            case Some(original) => Stream.raiseError[F](original)
            case None           => Stream.empty
          }
        }
    }

    /**
     * The background half of the design described on [[sourceAndAckImpl]]. `None` follows the last
     * window when the outer stream ends by itself. A source error is recorded in `sourceError`, not
     * propagated, because `concurrently` would interrupt the foreground on a background error.
     */
    private def sourceWindows(
      config: EventProcessingConfig[F],
      windows: Queue[F, Option[Window[F]]],
      sourceError: Deferred[F, Throwable]
    ): Stream[F, Nothing] = {
      val tokenedSources = for {
        s2 <- source.stream
        now <- Stream.eval(Sync[F].realTime)
        _ <- Stream.bracket(stateRef.set(InternalState.AwaitingUpstream(now)))(_ => stateRef.set(InternalState.Disconnected))
      } yield s2
        .through(monitorLatency(config, stateRef))
        .through(tokened(acksRef))
        .through(windowed(config.windowing))
        // Per inner stream, before `flatten` (see Invariants). `Stream.never` holds the outer
        // stream open until `concurrently` interrupts the background, and keeps `flatten` from
        // starting another inner stream.
        .handleErrorWith { e =>
          Stream.exec(recordSourceError(sourceError, e)) ++ Stream.never[F]
        }

      (tokenedSources.flatten.evalMap(w => windows.offer(Some(w))).drain ++ Stream.exec(windows.offer(None)))
        // An error in the outer stream. It has already finalized the outer stream and interrupted
        // any running inner stream.
        .handleErrorWith { e =>
          Stream.exec(recordSourceError(sourceError, e))
        }
    }

    def isHealthy(maxAllowedProcessingLatency: FiniteDuration): F[SourceAndAck.HealthStatus] =
      (stateRef.get, Sync[F].realTime).mapN {
        case (InternalState.Disconnected, _) =>
          SourceAndAck.Disconnected
        case (InternalState.AwaitingDownstream(since, _), now) if now - since > maxAllowedProcessingLatency =>
          SourceAndAck.LaggingEventProcessor(now - since)
        case (InternalState.AwaitingUpstream(since), now) if now - since > maxAllowedProcessingLatency =>
          SourceAndAck.InactiveSource(now - since)
        case _ => SourceAndAck.Healthy
      }

    def currentStreamLatency: F[Option[FiniteDuration]] =
      stateRef.get.flatMap {
        case InternalState.AwaitingDownstream(_, Some(tstamp)) =>
          Sync[F].realTime.map { now =>
            Some(now - tstamp)
          }
        case _ => none.pure[F]
      }
  }

  /**
   * The error the stream fails with when the foreground failed, so that the cause is reported, not
   * its consequences.
   *
   * With `numEagerWindows >= 1`, `parJoin` combines the windows' errors into an
   * `fs2.CompositeFailure`, in the order they reach it. An
   * [[EagerWindows.AbandonedWindowException]] is only a consequence of an earlier window's error.
   * But it can reach `parJoin` first: the failing window unblocks the next window from its own
   * finalizer, before its error has made its way out through [[CleanCancellation]]. So abandonments
   * are dropped whenever another error is present. The remaining errors keep the order `parJoin`
   * gave them, and a single one is raised on its own. Each window's error, abandonments included,
   * is still logged by [[CleanCancellation]].
   */
  private[internal] def withoutAbandonedWindows(e: Throwable): Throwable = {
    def flatten(t: Throwable): List[Throwable] =
      t match {
        case cf: CompositeFailure => cf.all.toList.flatMap(flatten)
        case other                => List(other)
      }
    flatten(e).filterNot(_.isInstanceOf[EagerWindows.AbandonedWindowException]) match {
      case Nil                     => e
      case single :: Nil           => single
      case first :: second :: rest => CompositeFailure(first, second, rest)
    }
  }

  // The release is best-effort, so its errors are only logged. Returns once the release is scheduled:
  // the supervised fiber can be cancelled before the release begins, but a release that begins
  // completes. The `started` flag and the call to `releaseUnemitted` must share one uncancelable
  // region. Then a fiber cancelled before it starts does not use up the run's single call, and the
  // first release to start is the one that runs. If they were split, a cancel landing after the flag
  // is set but before the call would use up the run's single call without releasing anything. No
  // test catches that split, because TestControl never cancels a fiber in that gap.
  private[internal] def startRelease[F[_]: Async](
    supervisor: Supervisor[F],
    started: Ref[F, Boolean],
    releaseUnemitted: F[Unit]
  ): F[Unit] =
    supervisor.supervise {
      started
        .getAndSet(true)
        .flatMap {
          case true => Sync[F].unit
          case false =>
            releaseUnemitted.handleErrorWith { e =>
              Logger[F].warn(e)(
                "Error releasing the source's un-emitted batches early. Shutdown continues, but the upstream may redeliver those batches later than an early release would have"
              )
            }
        }
        .uncancelable
    }.void

  private def recordSourceError[F[_]: Sync](sourceError: Deferred[F, Throwable], e: Throwable): F[Unit] =
    sourceError.complete(e).flatMap {
      case true =>
        Logger[F].error(e)("Error in the source of events. Waiting for open windows to finish before exiting")
      case false =>
        Sync[F].unit
    }

  private def nackUnhandled[F[_]: Monad, C](checkpointer: Checkpointer[F, C], ref: Ref[F, AcksState[C]]): F[Unit] =
    ref.get
      .flatMap { map =>
        checkpointer.nack(checkpointer.combineAll(map.values))
      }

  /**
   * An fs2 Pipe which caches the low-level checkpointable item, and replaces it with a token.
   *
   * The token can later be exchanged for the original checkpointable item
   */
  private def tokened[F[_]: Sync, C](ref: Ref[F, AcksState[C]]): Pipe[F, LowLevelEvents[C], TokenedEvents] =
    _.evalMap { case LowLevelEvents(events, ack, _) =>
      for {
        token <- Unique[F].unique
        _ <- ref.update(_ + (token -> ack))
      } yield TokenedEvents(events, token)
    }

  /**
   * An fs2 Pipe which records what time (duration since epoch) we last emitted a batch downstream
   * for processing
   */
  private def monitorLatency[F[_]: Sync, C](
    config: EventProcessingConfig[F],
    ref: Ref[F, InternalState]
  ): Pipe[F, Option[LowLevelEvents[C]], LowLevelEvents[C]] = {

    def go(source: Stream[F, Option[LowLevelEvents[C]]]): Pull[F, LowLevelEvents[C], Unit] =
      source.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((Some(pulled), source)) =>
          for {
            now <- Pull.eval(Sync[F].realTime)
            latency = pulled.earliestSourceTstamp.fold(Duration.Zero)(now - _)
            _ <- Pull.eval(config.latencyConsumer(latency))
            _ <- Pull.eval(ref.set(InternalState.AwaitingDownstream(now, pulled.earliestSourceTstamp)))
            _ <- Pull.output1(pulled)
            _ <- Pull.eval(ref.set(InternalState.AwaitingUpstream(now)))
            _ <- go(source)
          } yield ()
        case Some((None, source)) =>
          for {
            now <- Pull.eval(Sync[F].realTime)
            _ <- Pull.eval(config.latencyConsumer(Duration.Zero))
            _ <- Pull.eval(ref.set(InternalState.AwaitingUpstream(now)))
            _ <- go(source)
          } yield ()
      }

    source => go(source).stream
  }

  /**
   * An fs2 Pipe which feeds tokened messages into the [[EventProcessor]] and invokes the
   * checkpointer once they are processed
   *
   * @tparam F
   *   The effect type
   * @tparam C
   *   A checkpointable item, speficic to the stream
   * @param processor
   *   the [[EventProcessor]] provided by the application (e.g. Enrich or Transformer)
   * @param ref
   *   A map from tokens to checkpointable items. When the [[EventProcessor]] emits a token to
   *   signal completion of a batch of events, then we look up the checkpointable item from this
   *   map.
   * @param checkpointer
   *   Actions a ack/checkpoint when given a checkpointable action
   * @param control
   *   Controls the processing of eager windows. Waited on before each ack, and once at the end of
   *   the window's sink. Prevents this window from checkpointing any events before the previous
   *   window has finished successfully.
   * @param debounceCheckpoints
   *   Debounces how often we call the checkpointer.
   */
  private def messageSink[F[_]: Async, C](
    processor: EventProcessor[F],
    ref: Ref[F, AcksState[C]],
    checkpointer: Checkpointer[F, C],
    control: EagerWindows.Control[F],
    debounceCheckpoints: FiniteDuration
  ): Pipe[F, TokenedEvents, Nothing] =
    _.evalTap { case TokenedEvents(events, _) =>
      Logger[F].debug(s"Batch of ${events.size} events received from the source stream")
    }
      .through(processor)
      .chunks
      .evalMap { chunk =>
        chunk
          .traverse { token =>
            ref
              .modify { map =>
                (map - token, map.get(token))
              }
              .flatMap {
                case Some(c) => Async[F].pure(c)
                case None    => Async[F].raiseError[C](new IllegalStateException("Missing checkpoint for token"))
              }
          }
          .map { cs =>
            checkpointer.combineAll(cs.toIterable)
          }
      }
      .prefetch // This prefetch means we can ack messages concurrently with processing the next batch
      .through(batchUpCheckpoints(debounceCheckpoints, checkpointer))
      .evalTap(_ => control.waitForPreviousWindow)
      .evalMap(c => checkpointer.ack(c))
      .drain
      .append(Stream.exec(control.waitForPreviousWindow))
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          control.unblockNextWindow(EagerWindows.PreviousWindowSuccess)
        case ExitCase.Canceled | ExitCase.Errored(_) =>
          control.unblockNextWindow(EagerWindows.PreviousWindowFailed)
      }

  private def windowed[F[_]: Async, A](config: EventProcessingConfig.Windowing): Pipe[F, A, Queue[F, Option[A]]] =
    config match {
      case EventProcessingConfig.NoWindowing      => singleWindow
      case tw: EventProcessingConfig.TimedWindows => timedWindows(tw)
    }

  /**
   * An fs2 Pipe which emits a single window, which ends when the input ends. Used for
   * `NoWindowing`, so one window per inner stream.
   *
   * The input is evaluated here, in the background, not by the window's consumer. An error raised
   * by the input passes straight through, without ending the window.
   */
  private def singleWindow[F[_]: Async, A]: Pipe[F, A, Queue[F, Option[A]]] =
    in =>
      Stream.eval(Queue.synchronous[F, Option[A]]).flatMap { q =>
        Stream.emit(q) ++
          in.evalMap(a => q.offer(Some(a))).drain ++
          Stream.exec(q.offer(None))
      }

  private def eagerness(config: EventProcessingConfig.Windowing): Int =
    config match {
      case EventProcessingConfig.NoWindowing      => 1
      case tw: EventProcessingConfig.TimedWindows => tw.numEagerWindows + 1
    }

  /**
   * An fs2 Pipe which converts a stream of `A` into a stream of windows. Each window in the output
   * provides events over a fixed window of time. When the window is over, or the input ends, `None`
   * is enqueued. The input is evaluated and errors pass through as for [[singleWindow]].
   *
   * For an [[EventProcessor]], the end of the stream it consumes is a signal to cleanly handle the
   * end of a window. For example, the AWS Transformer would handle it by flushing all pending
   * events to S3 and then sending the SQS message.
   */
  private def timedWindows[F[_]: Async, A](config: EventProcessingConfig.TimedWindows): Pipe[F, A, Queue[F, Option[A]]] = {
    def go(
      timedPull: Pull.Timed[F, A],
      current: Option[Queue[F, Option[A]]],
      nextDuration: FiniteDuration
    ): Pull[F, Queue[F, Option[A]], Unit] =
      timedPull.uncons.flatMap {
        case None =>
          current match {
            case None    => Pull.done
            case Some(q) => Pull.eval(q.offer(None)) >> Pull.done
          }
        case Some((Left(_), next)) =>
          current match {
            case None    => go(next, None, nextDuration)
            case Some(q) => Pull.eval(q.offer(None)) >> go(next, None, nextDuration)
          }
        case Some((Right(chunk), next)) =>
          current match {
            case None =>
              val pull = for {
                q <- Pull.eval(Queue.synchronous[F, Option[A]])
                _ <- Pull.output1(q)
                _ <- Pull.eval(chunk.traverse(a => q.offer(Some(a))))
                _ <- Pull.eval(Logger[F].info(s"Opening new window with duration $nextDuration")) >> next.timeout(nextDuration)
              } yield go(next, Some(q), (nextDuration * 2).min(config.duration))
              pull.flatten
            case Some(q) =>
              Pull.eval(chunk.traverse(a => q.offer(Some(a)))) >> go(next, Some(q), nextDuration)
          }
      }

    in =>
      in.pull.timed { timedPull: Pull.Timed[F, A] =>
        val timeout = timeoutForFirstWindow(config)
        val pull = for {
          _ <- Pull.eval(Logger[F].info(s"Opening first window with randomly adjusted duration of $timeout"))
          _ <- timedPull.timeout(timeout)
          q <- Pull.eval(Queue.synchronous[F, Option[A]])
          _ <- Pull.output1(q)
        } yield go(timedPull, Some(q), (2 * timeout).min(config.duration))
        pull.flatten
      }.stream
  }

  /**
   * When the application first starts up, the first timed window should have a random size.
   *
   * This addresses the situation where several parallel instances of the app all start at the same
   * time. All instances in the group should end windows at slightly different times, so that
   * downstream gets a more steady flow of completed batches.
   */
  private def timeoutForFirstWindow(config: EventProcessingConfig.TimedWindows): FiniteDuration =
    (config.duration.toMillis * config.firstWindowScaling).toLong.milliseconds

  private def batchUpCheckpoints[F[_]: Async, C](timeout: FiniteDuration, semigroup: Semigroup[C]): Pipe[F, C, C] = {

    def go(timedPull: Pull.Timed[F, C], output: Option[C]): Pull[F, C, Unit] =
      timedPull.uncons.flatMap {
        case None =>
          // Upstream finished cleanly. Emit whatever is pending and we're done.
          Pull.outputOption1(output)
        case Some((Left(_), next)) =>
          // Timer timed-out. Emit whatever is pending.
          Pull.outputOption1(output) >> go(next, None)
        case Some((Right(chunk), next)) =>
          // Upstream emitted tokens to us. We might already have pending tokens
          output match {
            case Some(c) =>
              go(next, Some(chunk.foldLeft(c)(semigroup.combine(_, _))))
            case None =>
              semigroup.combineAllOption(chunk.iterator) match {
                case Some(c) =>
                  next.timeout(timeout) >> go(next, Some(c))
                case None =>
                  go(next, None)
              }
          }
      }

    in =>
      if (timeout > Duration.Zero)
        in.pull.timed { timedPull =>
          go(timedPull, None)
        }.stream
      else
        in
  }
}
