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
import cats.effect.std.Queue
import cats.effect.kernel.{Ref, Unique}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.{Async, Sync}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}

/**
 * A common interface over external sources of events
 *
 * This library uses [[LowLevelSource]] internally as a stepping stone towards implementing a
 * [[SourceAndAckk]].
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
   * The inner streams are processed one at a time, with clean separation before starting the next
   * inner stream. This is required e.g. for Kafka, where the end of a stream represents client
   * rebalancing.
   *
   * A new [[EventProcessor]] will be invoked for each inner stream
   *
   * The inner stream should periodically emit `None` as a signal that it is alive and healthy, even
   * when there are no events on the stream. Failure to emit frequently will result in the
   * `SourceAndAck` reporting itself as unhealthy.
   */
  def stream: Stream[F, Stream[F, Option[LowLevelEvents[C]]]]

  /**
   * How frequently we should checkpoint progress to this source
   *
   * E.g. for the Kinesis we can increase value to reduce how often we need to write to the DynamoDB
   * table
   */
  def debounceCheckpoints: FiniteDuration
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

  private def sourceAndAckImpl[F[_]: Async, C](
    source: LowLevelSource[F, C],
    stateRef: Ref[F, InternalState],
    acksRef: Ref[F, Map[Unique.Token, C]]
  ): SourceAndAck[F] = new SourceAndAck[F] {
    def stream(config: EventProcessingConfig[F], processor: EventProcessor[F]): Stream[F, Nothing] = {
      val sinks = EagerWindows.pipes { control: EagerWindows.Control[F] =>
        CleanCancellation(messageSink(processor, acksRef, source.checkpointer, control, source.debounceCheckpoints))
      }

      val tokenedSources = for {
        s2 <- source.stream
        now <- Stream.eval(Sync[F].realTime)
        _ <- Stream.bracket(stateRef.set(InternalState.AwaitingUpstream(now)))(_ => stateRef.set(InternalState.Disconnected))
      } yield s2
        .through(monitorLatency(config, stateRef))
        .through(tokened(acksRef))
        .through(windowed(config.windowing))

      tokenedSources.flatten
        .zip(sinks)
        .map { case (tokenedSource, sink) => sink(tokenedSource) }
        .parJoin(eagerness(config.windowing)) // so we start processing the next window while the previous window is still finishing up.
        .onFinalize {
          nackUnhandled(source.checkpointer, acksRef)
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
   *   Controls the processing of eager windows. Prevents the next eager window from checkpointing
   *   any events before the previous window is fully finalized.
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
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          control.unblockNextWindow(EagerWindows.PreviousWindowSuccess)
        case ExitCase.Canceled | ExitCase.Errored(_) =>
          control.unblockNextWindow(EagerWindows.PreviousWindowFailed)
      }

  private def windowed[F[_]: Async, A](config: EventProcessingConfig.Windowing): Pipe[F, A, Stream[F, A]] =
    config match {
      case EventProcessingConfig.NoWindowing      => in => Stream.emit(in)
      case tw: EventProcessingConfig.TimedWindows => timedWindows(tw)
    }

  private def eagerness(config: EventProcessingConfig.Windowing): Int =
    config match {
      case EventProcessingConfig.NoWindowing      => 1
      case tw: EventProcessingConfig.TimedWindows => tw.numEagerWindows + 1
    }

  /**
   * An fs2 Pipe which converts a stream of `A` into a stream of `Stream[F, A]`. Each stream in the
   * output provides events over a fixed window of time. When the window is over, the inner stream
   * terminates with success.
   *
   * For an [[EventProcessor]], termination of the inner stream is a signal to cleanly handle the
   * end of a window. For example, the AWS Transformer would handle termination of the inner stream
   * by flushing all pending events to S3 and then sending the SQS message.
   */
  private def timedWindows[F[_]: Async, A](config: EventProcessingConfig.TimedWindows): Pipe[F, A, Stream[F, A]] = {
    def go(
      timedPull: Pull.Timed[F, A],
      current: Option[Queue[F, Option[A]]],
      nextDuration: FiniteDuration
    ): Pull[F, Stream[F, A], Unit] =
      timedPull.uncons.attempt.flatMap {
        case Right(None) =>
          current match {
            case None    => Pull.done
            case Some(q) => Pull.eval(q.offer(None)) >> Pull.done
          }
        case Right(Some((Left(_), next))) =>
          current match {
            case None    => go(next, None, nextDuration)
            case Some(q) => Pull.eval(q.offer(None)) >> go(next, None, nextDuration)
          }
        case Right(Some((Right(chunk), next))) =>
          current match {
            case None =>
              val pull = for {
                q <- Pull.eval(Queue.synchronous[F, Option[A]])
                _ <- Pull.output1(Stream.fromQueueNoneTerminated(q))
                _ <- Pull.eval(chunk.traverse(a => q.offer(Some(a))))
                _ <- Pull.eval(Logger[F].info(s"Opening new window with duration $nextDuration")) >> next.timeout(nextDuration)
              } yield go(next, Some(q), (nextDuration * 2).min(config.duration))
              pull.flatten
            case Some(q) =>
              Pull.eval(chunk.traverse(a => q.offer(Some(a)))) >> go(next, Some(q), nextDuration)
          }
        case Left(throwable) =>
          current match {
            case None    => Pull.raiseError[F](throwable)
            case Some(q) => Pull.eval(q.offer(None)) >> Pull.raiseError[F](throwable)
          }
      }

    in =>
      in.pull
        .timed { timedPull: Pull.Timed[F, A] =>
          val timeout = timeoutForFirstWindow(config)
          val pull = for {
            _ <- Pull.eval(Logger[F].info(s"Opening first window with randomly adjusted duration of $timeout"))
            _ <- timedPull.timeout(timeout)
            q <- Pull.eval(Queue.synchronous[F, Option[A]])
            _ <- Pull.output1(Stream.fromQueueNoneTerminated(q))
          } yield go(timedPull, Some(q), (2 * timeout).min(config.duration))
          pull.flatten
        }
        .stream
        .prefetch // This prefetch is required to pull items into the emitted stream
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
