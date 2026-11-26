/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.effect.{IO, Outcome}
import cats.effect.kernel.Ref
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import fs2.{Chunk, Stream}
import org.specs2.Specification

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor}

/**
 * Part of `LowLevelSourceSpec`, in its own class because specs2's `s2` interpolator embeds the
 * spec's source text in a single string constant, and `LowLevelSourceSpec` is at the JVM's 64 KB
 * limit for one.
 */
class LowLevelSourceEmptyWindowsSpec extends Specification with CatsEffect {
  import LowLevelSourceSpec.Action
  import LowLevelSourceEmptyWindowsSpec._

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck, with eager windows, when an inner stream ends with no data, should:
    ack a later window only after the window before the empty one has finished $empty1
    never ack a later window when the window before the empty one failed $empty2
    never ack a later window when the window before two empty ones failed $empty3
    never ack a later window when the window before the empty one failed, with two eager windows $empty2Eager2
    never ack a later window when the window before two empty ones failed, with three eager windows $empty3Eager3
    exit cleanly, with acks in order, when cancelled while the empty window waits for the window before it $empty4

  A LowLevelSource raised to a SourceAndAck, with eager windows, when an inner stream's first window times out with no data, should:
    ack a later window only after the window before the empty one has finished $timedOut1
    never ack a later window when the window before the empty one failed $timedOut2
    never ack a later window when the window before the empty one failed, with two eager windows $timedOut2Eager2
  """

  def empty1 = {
    val io = run(List(List("1"), Nil, List("2")), Map(1 -> WindowEnd.Slow))

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions must beEqualTo(
        UntilEmptyWindowEnds ++ Vector(
          Action.Checkpointed(List("1")),
          Action.ProcessorStartedWindow("1970-01-01T00:00:31Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:31Z", List("2")),
          Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:31Z"),
          Action.Checkpointed(List("2")),
          Action.Nacked(Nil)
        )
      )) and (error must beNone)
    }
  }

  // At 31s, window 1's error and the window after the empty one race, so only what must hold is
  // asserted after the prefix
  def empty2 = failedBeforeEmpty(List(List("1"), Nil, List("2")), numEagerWindows = 1)

  def empty3 = failedBeforeEmpty(List(List("1"), Nil, Nil, List("2")), numEagerWindows = 1)

  // With enough eager windows for window 1, the empty windows and the window after them to run at
  // once, that window starts while window 1 is still finishing, so it reaches its ack before window
  // 1 fails. Only the chain stops it from acking.
  def empty2Eager2 = failedBeforeEmpty(List(List("1"), Nil, List("2")), numEagerWindows = 2)

  def empty3Eager3 = failedBeforeEmpty(List(List("1"), Nil, Nil, List("2")), numEagerWindows = 3)

  private def failedBeforeEmpty(innerStreams: List[List[String]], numEagerWindows: Int) = {
    val io = run(innerStreams, Map(1 -> WindowEnd.Failing), numEagerWindows = numEagerWindows)

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions.take(UntilEmptyWindowEnds.size) must beEqualTo(UntilEmptyWindowEnds)) and
        (checkpoints(actions) must beEmpty) and
        (error must beSome(WindowError))
    }
  }

  // Cancelled at 10s, while window 1 is finishing and the empty window waits for it. Once the
  // empty window frees its slot at 31s, `parJoin` can still start the next window during shutdown,
  // and that window may take "2" and ack it, in order, before it is ended.
  def empty4 = {
    val io = run(List(List("1"), Nil, List("2")), Map(1 -> WindowEnd.Slow), cancelAfter = 10.seconds)

    TestControl.executeEmbed(io).map { case (actions, error) =>
      val prefix = UntilEmptyWindowEnds :+ Action.Checkpointed(List("1"))
      (actions.take(prefix.size) must beEqualTo(prefix)) and
        (checkpoints(actions) must beOneOf(Vector(List("1")), Vector(List("1"), List("2")))) and
        (error must beNone)
    }
  }

  // The second inner stream's first window opens at 1.5s and times out empty at 11.5s. "2" arrives
  // at 15s, in the same inner stream, while window 1 is still finishing. The source is
  // back-pressured, so the next window takes "2" only once window 1 has finished at 31s, and the
  // inner stream, and with it that window, ends a second later.
  def timedOut1 = {
    val io = run(List(List("1"), List("2")), Map(1 -> WindowEnd.Slow), quietStarts = Map(2 -> 13500.millis))

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions must beEqualTo(
        UntilTimedOutWindowEnds ++ Vector(
          Action.Checkpointed(List("1")),
          Action.ProcessorStartedWindow("1970-01-01T00:00:31Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:31Z", List("2")),
          Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:32Z"),
          Action.Checkpointed(List("2")),
          Action.Nacked(Nil)
        )
      )) and (error must beNone)
    }
  }

  def timedOut2 = timedOutAfterFailed(numEagerWindows = 1)

  def timedOut2Eager2 = timedOutAfterFailed(numEagerWindows = 2)

  private def timedOutAfterFailed(numEagerWindows: Int) = {
    val io = run(
      List(List("1"), List("2")),
      Map(1 -> WindowEnd.Failing),
      quietStarts     = Map(2 -> 13500.millis),
      numEagerWindows = numEagerWindows
    )

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions.take(UntilTimedOutWindowEnds.size) must beEqualTo(UntilTimedOutWindowEnds)) and
        (checkpoints(actions) must beEmpty) and
        (error must beSome(WindowError))
    }
  }
}

object LowLevelSourceEmptyWindowsSpec {
  import LowLevelSourceSpec.Action

  val WindowError = "window failed"

  /** What got acked, in order */
  def checkpoints(actions: Vector[Action]): Vector[List[String]] =
    actions.collect { case Action.Checkpointed(c) => c }

  /**
   * Window 1 with batch "1", then the empty window. The `empty*` tests' actions start with these.
   */
  val UntilEmptyWindowEnds: Vector[Action] = Vector(
    Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
    Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1")),
    Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:01Z"),
    Action.ProcessorStartedWindow("1970-01-01T00:00:01.500Z"),
    Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:01.500Z")
  )

  /**
   * Window 1 with batch "1", then the second inner stream's first window, which times out empty.
   * The `timedOut*` tests' actions start with these.
   */
  val UntilTimedOutWindowEnds: Vector[Action] = Vector(
    Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
    Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1")),
    Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:01Z"),
    Action.ProcessorStartedWindow("1970-01-01T00:00:01.500Z"),
    Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:11.500Z")
  )

  sealed trait WindowEnd
  object WindowEnd {

    /** Takes 30 seconds after the end of its window before emitting its tokens */
    case object Slow extends WindowEnd

    /** Fails 30 seconds after the end of its window, without emitting its tokens */
    case object Failing extends WindowEnd
  }

  /**
   * Runs a source whose inner streams emit the given events, one per batch, each followed by one
   * second, then end. The next inner stream starts half a second later, so that no two windows'
   * actions happen at the same time. The outer stream then stays open. The stream is cancelled
   * after `cancelAfter`, as on SIGTERM, if it is still running.
   *
   * @param windowEnds
   *   How the window with this 1-based index ends. Other windows emit their tokens at once.
   * @param quietStarts
   *   How long the inner stream with this 1-based index waits before its first event.
   * @param numEagerWindows
   *   As in `EventProcessingConfig.TimedWindows`, whose windows are 10 seconds long.
   * @return
   *   The recorded actions, and the message of the error the stream failed with, if any
   */
  def run(
    innerStreams: List[List[String]],
    windowEnds: Map[Int, WindowEnd],
    cancelAfter: FiniteDuration           = 1.minute,
    quietStarts: Map[Int, FiniteDuration] = Map.empty,
    numEagerWindows: Int                  = 1
  ): IO[(Vector[Action], Option[String])] =
    for {
      ref <- Ref[IO].of(Vector.empty[Action])
      windowCounter <- Ref[IO].of(0)
      sourceAndAck <- LowLevelSource.toSourceAndAck(source(ref, innerStreams, quietStarts))
      config =
        EventProcessingConfig(EventProcessingConfig.TimedWindows(10.seconds, 1.0, numEagerWindows), (_: FiniteDuration) => IO.unit)
      fiber <- sourceAndAck.stream(config, processor(ref, windowCounter, windowEnds)).compile.drain.start
      _ <- IO.sleep(cancelAfter)
      _ <- fiber.cancel
      outcome <- fiber.join
      result <- ref.get
    } yield {
      val error = outcome match {
        case Outcome.Errored(e) => Some(e.getMessage)
        case _                  => None
      }
      (result, error)
    }

  /** `LowLevelSourceSpec.windowedProcessor`, with each window ending as `windowEnds` says */
  def processor(
    ref: Ref[IO, Vector[Action]],
    windowCounter: Ref[IO, Int],
    windowEnds: Map[Int, WindowEnd]
  ): EventProcessor[IO] = { in =>
    val config = LowLevelSourceSpec.TestSourceConfig(
      batchesPerRebalance = 1,
      eventsPerBatch      = 1,
      timeBetweenBatches  = 1.second,
      timeToProcessBatch  = 0.seconds
    )
    def windowed(timeToFinalizeWindow: FiniteDuration) =
      LowLevelSourceSpec.windowedProcessor(ref, config.copy(timeToFinalizeWindow = timeToFinalizeWindow))(in)

    Stream.eval(windowCounter.updateAndGet(_ + 1)).flatMap { index =>
      windowEnds.get(index) match {
        case Some(WindowEnd.Slow)    => windowed(30.seconds)
        case Some(WindowEnd.Failing) => windowed(30.seconds).drain ++ Stream.raiseError[IO](new RuntimeException(WindowError))
        case None                    => windowed(0.seconds)
      }
    }
  }

  /** A source with the given inner streams. It records what gets acked and nacked. */
  def source(
    ref: Ref[IO, Vector[Action]],
    innerStreams: List[List[String]],
    quietStarts: Map[Int, FiniteDuration]
  ): LowLevelSource[IO, List[String]] =
    new NoReleaseLowLevelSource[List[String]] {
      def checkpointer: Checkpointer[IO, List[String]] = new Checkpointer[IO, List[String]] {
        def ack(c: List[String]): IO[Unit]                          = IO.sleep(1.nanos) >> ref.update(_ :+ Action.Checkpointed(c))
        def nack(c: List[String]): IO[Unit]                         = ref.update(_ :+ Action.Nacked(c.sortBy(_.toInt)))
        def empty: List[String]                                     = Nil
        def combine(x: List[String], y: List[String]): List[String] = x ++ y
      }

      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[List[String]]]]] =
        Stream.emits(innerStreams.zipWithIndex).flatMap { case (events, i) =>
          val quietStart = quietStarts.get(i + 1).fold[Stream[IO, Nothing]](Stream.empty)(Stream.sleep_[IO](_))
          val inner = quietStart ++ Stream.emits(events).flatMap { e =>
            val asBytes = Chunk.singleton(ByteBuffer.wrap(e.getBytes(StandardCharsets.UTF_8)))
            Stream.emit(Some(LowLevelEvents(events = asBytes, ack = List(e), earliestSourceTstamp = None))) ++
              Stream.sleep_[IO](1.second)
          }
          Stream.emit(inner) ++ Stream.sleep_[IO](500.millis)
        } ++ Stream.never[IO]

      def debounceCheckpoints: FiniteDuration = 1.millis
    }
}
