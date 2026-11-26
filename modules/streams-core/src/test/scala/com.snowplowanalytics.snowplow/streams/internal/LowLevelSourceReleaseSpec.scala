/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.implicits._
import cats.effect.{IO, Resource}
import cats.effect.std.Supervisor
import cats.effect.kernel.{Ref, Unique}
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import fs2.{Chunk, Stream}
import org.specs2.Specification
import org.specs2.execute.Result

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.TimeoutException

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, TokenedEvents}

class LowLevelSourceReleaseSpec extends Specification with CatsEffect {
  import LowLevelSourceReleaseSpec._

  // Longer than `forSeeds`'s own real-time limit, so that limit reports a hang as a failure. When
  // cats-effect-testing's timeout fires instead, specs2 reports the example as skipped.
  override val Timeout: Duration = 2.minutes

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck, when cancelled, should:
    release un-emitted batches before the processor finishes finalizing, with no windowing $release1
    release un-emitted batches before the processor finishes finalizing, with a timed window $release2
    release un-emitted batches, calling the release once, when one window is finalizing and the next is open $release3
    finish cancellation, and lose no batch, when the release raises an error $release4
    not wait for the release to end the window, but complete the stream only after the release $release5
    complete the release before the source's resource is released $release8
  A LowLevelSource raised to a SourceAndAck, when the processor raises an error, should:
    fail with that error, release un-emitted batches from the cancelled sibling window, and lose no batch $release6
    fail with that error, and lose no batch, whether or not the release runs $release9
    call the release when a later window schedules it, if an earlier window's release was cancelled before it started $release10
  A LowLevelSource raised to a SourceAndAck, when its windows end without cancellation, should:
    not release un-emitted batches $release7
  """

  def release1 = cancelAndCheck(EventProcessingConfig.NoWindowing, ReleaseBehaviour.Normal)

  def release2 = cancelAndCheck(EventProcessingConfig.TimedWindows(10.minutes, 1.0, 2), ReleaseBehaviour.Normal)

  // Windows of 20 seconds, each taking 60 seconds to finalize. At the 30-second cancellation, the
  // first window is still finalizing and the second is open, so both cancelled windows schedule the
  // release, and the source still sees a single call.
  def release3 = cancelAndCheck(EventProcessingConfig.TimedWindows(20.seconds, 1.0, 4), ReleaseBehaviour.Normal)

  def release4 = cancelAndCheck(EventProcessingConfig.TimedWindows(10.minutes, 1.0, 2), ReleaseBehaviour.Raise)

  // The release takes 100 seconds, and the processor 60 seconds to finalize its window. So the
  // processor finishes before the release does, and the stream completes only after the release.
  def release5 = {
    val cancelAfter = 30.seconds
    forSeeds {
      runInsideSourceResource(ReleaseBehaviour.Slow(100.seconds), cancelAfter).map { case (result, finalState) =>
        val finishes        = result.collect { case Action.ProcessorFinished(t) => t }
        val releaseFinishes = result.collect { case Action.ReleaseFinished(t) => t }
        val streamEnds      = result.collect { case Action.StreamCompleted(t) => t }
        (result.collect { case Action.ReleaseCalled(t) => t } must beEqualTo(Vector(cancelAfter))).toResult and
          (finishes must not(beEmpty)).toResult and
          (finishes must contain(be_<(cancelAfter + 100.seconds)).forall).toResult and
          (releaseFinishes must beEqualTo(Vector(cancelAfter + 100.seconds))).toResult and
          (streamEnds must beEqualTo(Vector(cancelAfter + 100.seconds))).toResult and
          checkExactlyOnce(result, finalState)
      }
    }
  }

  // Eager windows of 20 seconds. The first window fails 30 seconds after it ends, while the second
  // window is open, so `parJoin` cancels the second window, which starts the release.
  def release6 = {
    val io = processorFails(laterWindowsFinalize = Some(60.seconds)).map { case (outcome, result, finalState) =>
      val releaseCalls = result.collect { case Action.ReleaseCalled(t) => t }
      val released     = result.collect { case Action.Released(ids) => ids }.flatten
      (outcome.leftMap(_.getMessage) must beLeft("boom! processor failed")).toResult and
        (releaseCalls.size must beEqualTo(1)).toResult and
        (released must not(beEmpty)).toResult and
        checkExactlyOnce(result, finalState)
    }

    forSeeds(io)
  }

  // As `release6`, but the cancelled sibling window can finish its drain at the instant it schedules
  // the release, so on some orderings the supervisor cancels the release before it begins. Either
  // way, every batch is processed or given back exactly once. Such orderings are rare, so this runs
  // over more seeds than the other examples.
  def release9 = {
    val io = processorFails(laterWindowsFinalize = None).map { case (outcome, result, finalState) =>
      (outcome.leftMap(_.getMessage) must beLeft("boom! processor failed")).toResult and
        checkExactlyOnce(result, finalState)
    }

    forSeeds(io, seeds = 40)
  }

  // The first release is scheduled on a supervisor that is closed at once. Under TestControl, which
  // `forSeeds` always uses, the forked fiber does not run until the test's own fiber yields, so
  // closing the supervisor cancels it before it starts. On the real runtime the fiber often runs
  // first, so this test relies on TestControl. A release scheduled later in the same run must still
  // call the source.
  def release10 =
    forSeeds {
      for {
        started <- Ref[IO].of(false)
        calls <- Ref[IO].of(Vector.empty[String])
        _ <- Supervisor[IO](await = false).use { supervisor =>
               LowLevelSource.startRelease(supervisor, started, calls.update(_ :+ "first"))
             }
        _ <- Supervisor[IO](await = true).use { supervisor =>
               LowLevelSource.startRelease(supervisor, started, calls.update(_ :+ "second"))
             }
        result <- calls.get
      } yield result must beEqualTo(Vector("second"))
    }

  private def processorFails(laterWindowsFinalize: Option[FiniteDuration]): IO[(Either[Throwable, Unit], Vector[Action], FakeState)] = {
    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(20.seconds, 1.0, 2), _ => IO.unit)
    for {
      log <- Ref[IO].of(Vector.empty[Action])
      state <- Ref[IO].of(FakeState.initial)
      sourceAndAck <- LowLevelSource.toSourceAndAck(bufferingSource(log, state, ReleaseBehaviour.Normal))
      windows <- Ref[IO].of(0)
      outcome <- sourceAndAck.stream(config, failingProcessor(windows, laterWindowsFinalize)).compile.drain.attempt
      result <- log.get
      finalState <- state.get
    } yield (outcome, result, finalState)
  }

  // Windows of 10 seconds, each finished at once by the processor. Several windows end normally
  // before the cancellation at 45 seconds, and none of them may start the release.
  def release7 = {
    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(10.seconds, 1.0, 2), _ => IO.unit)

    val io = for {
      log <- Ref[IO].of(Vector.empty[Action])
      state <- Ref[IO].of(FakeState.initial)
      sourceAndAck <- LowLevelSource.toSourceAndAck(bufferingSource(log, state, ReleaseBehaviour.Normal))
      fiber <- sourceAndAck.stream(config, _.map(_.ack)).compile.drain.start
      _ <- IO.sleep(45.seconds)
      beforeCancel <- log.get
      _ <- fiber.cancel
    } yield (beforeCancel.collect { case Action.ReleaseCalled(t) => t } must beEmpty).toResult and
      (beforeCancel.collect { case Action.Acked(ids) => ids }.flatten must not(beEmpty)).toResult

    forSeeds(io)
  }

  def release8 =
    forSeeds {
      runInsideSourceResource(ReleaseBehaviour.Slow(100.seconds), 30.seconds).map { case (result, _) =>
        val order = result.collect {
          case Action.ReleaseFinished(_)        => "release finished"
          case Action.SourceResourceReleased(_) => "source resource released"
        }
        order must beEqualTo(Vector("release finished", "source resource released"))
      }
    }

  /**
   * Runs the stream inside a resource standing for the source's own, as apps do, and cancels the
   * whole run. Logs when the stream completes and when the resource is released.
   */
  private def runInsideSourceResource(behaviour: ReleaseBehaviour, cancelAfter: FiniteDuration): IO[(Vector[Action], FakeState)] = {
    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)
    for {
      log <- Ref[IO].of(Vector.empty[Action])
      state <- Ref[IO].of(FakeState.initial)
      logAt          = (f: FiniteDuration => Action) => IO.realTime.flatMap(t => log.update(_ :+ f(t)))
      sourceResource = Resource.make(IO.unit)(_ => logAt(Action.SourceResourceReleased(_)))
      fiber <- sourceResource.use { _ =>
                 LowLevelSource.toSourceAndAck(bufferingSource(log, state, behaviour)).flatMap { sourceAndAck =>
                   sourceAndAck.stream(config, slowProcessor(log)).compile.drain.guarantee(logAt(Action.StreamCompleted(_)))
                 }
               }.start
      _ <- IO.sleep(cancelAfter)
      _ <- fiber.cancel
      result <- log.get
      finalState <- state.get
    } yield (result, finalState)
  }

  private def cancelAndCheck(
    windowing: EventProcessingConfig.Windowing,
    behaviour: ReleaseBehaviour
  ) = {
    val config      = EventProcessingConfig(windowing, _ => IO.unit)
    val cancelAfter = 30.seconds

    val io = for {
      log <- Ref[IO].of(Vector.empty[Action])
      state <- Ref[IO].of(FakeState.initial)
      sourceAndAck <- LowLevelSource.toSourceAndAck(bufferingSource(log, state, behaviour))
      processor = slowProcessor(log)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(cancelAfter)
      _ <- fiber.cancel
      result <- log.get
      finalState <- state.get
    } yield {
      val releaseCalls = result.collect { case Action.ReleaseCalled(t) => t }
      val finishes     = result.collect { case Action.ProcessorFinished(t) => t }
      val released     = result.collect { case Action.Released(ids) => ids }.flatten

      // Batches released before any window finished finalizing after the cancellation
      val releasedEarly = result
        .takeWhile {
          case Action.ProcessorFinished(t) => t <= cancelAfter
          case _                           => true
        }
        .collect { case Action.Released(ids) => ids }
        .flatten

      val releaseCalledAtCancel =
        (releaseCalls must beEqualTo(Vector(cancelAfter))).toResult and
          (finishes.filter(_ > cancelAfter) must not(beEmpty)).toResult

      val outcome = behaviour match {
        case ReleaseBehaviour.Normal | ReleaseBehaviour.Slow(_) =>
          // The source buffers a batch every 100ms, and the processor takes one per second
          (releasedEarly.size must be_>=(200)).toResult and (released must beEqualTo(releasedEarly)).toResult
        case _ =>
          (released must beEmpty).toResult
      }

      releaseCalledAtCancel and outcome and checkExactlyOnce(result, finalState)
    }

    forSeeds(io)
  }

  /**
   * TestControl orders tasks due at the same instant by a seed, so check several orderings.
   *
   * A program that never finishes does not always end TestControl with a non-termination error: it
   * can keep TestControl busy at one instant of virtual time, which neither a virtual-time timeout
   * nor cancellation interrupts. So the limit is on real time, and on timeout the run is abandoned
   * rather than cancelled.
   */
  private def forSeeds(io: IO[Result], seeds: Int = 8): IO[Result] =
    (1 to seeds).toList
      .traverse { seed =>
        val encoded = Base64.getEncoder.encodeToString((seed * 7919L).toString.getBytes)
        TestControl.executeEmbed(io, seed = Some(encoded)).map(_.mapMessage(m => s"seed $seed: $m"))
      }
      .map(_.reduce(_ and _))
      .timeoutAndForget(1.minute)
      .recover { case _: TimeoutException => failure("Did not finish within 1 minute of real time") }

  /** Every batch the source received was acked, nacked or released, and only once */
  private def checkExactlyOnce(result: Vector[Action], finalState: FakeState): Result = {
    val settled = result.collect {
      case Action.Acked(ids)    => ids
      case Action.Nacked(ids)   => ids
      case Action.Released(ids) => ids
    }.flatten
    (settled.sorted must beEqualTo(settled.distinct.sorted)).toResult and
      (settled.toSet must beEqualTo((1 to finalState.received).toSet)).toResult and
      (finalState.batches must beEmpty).toResult
  }
}

object LowLevelSourceReleaseSpec {

  sealed trait Action
  object Action {
    case class ReleaseCalled(at: FiniteDuration) extends Action
    case class ProcessorFinished(at: FiniteDuration) extends Action
    case class Acked(ids: List[Int]) extends Action
    case class Nacked(ids: List[Int]) extends Action
    case class Released(ids: List[Int]) extends Action
    case class ReleaseFinished(at: FiniteDuration) extends Action
    case class StreamCompleted(at: FiniteDuration) extends Action
    case class SourceResourceReleased(at: FiniteDuration) extends Action
  }

  sealed trait ReleaseBehaviour
  object ReleaseBehaviour {
    case object Normal extends ReleaseBehaviour
    case object Raise extends ReleaseBehaviour
    case class Slow(duration: FiniteDuration) extends ReleaseBehaviour
  }

  sealed trait Status
  object Status {
    case object Buffered extends Status
    case object Emitted extends Status
  }

  /** Mirrors the claim/release state of the pubsub source */
  case class FakeState(
    isOpen: Boolean,
    received: Int,
    batches: Map[Int, Status]
  ) {
    def remove(ids: List[Int]): (FakeState, List[Int]) =
      (copy(batches = batches -- ids), ids.filter(batches.contains))
  }

  object FakeState {
    val initial: FakeState = FakeState(isOpen = true, received = 0, Map.empty)
  }

  /**
   * A source with a large internal buffer, which receives a batch from upstream more quickly than
   * the processor can handle it.
   */
  def bufferingSource(
    log: Ref[IO, Vector[Action]],
    state: Ref[IO, FakeState],
    behaviour: ReleaseBehaviour
  ): LowLevelSource[IO, List[Int]] =
    new LowLevelSource[IO, List[Int]] {

      override def checkpointer: Checkpointer[IO, List[Int]] = new Checkpointer[IO, List[Int]] {
        override def ack(c: List[Int]): IO[Unit] =
          state.modify(_.remove(c)).flatMap(ids => log.update(_ :+ Action.Acked(ids)).whenA(ids.nonEmpty))
        override def nack(c: List[Int]): IO[Unit] =
          state.modify(_.remove(c)).flatMap(ids => log.update(_ :+ Action.Nacked(ids)).whenA(ids.nonEmpty))
        override def empty: List[Int]                               = Nil
        override def combine(x: List[Int], y: List[Int]): List[Int] = x ++ y
      }

      private val receive: IO[Option[Int]] =
        state
          .modify { s =>
            val id = s.received + 1
            if (s.isOpen) (s.copy(received = id, batches = s.batches + (id -> Status.Buffered)), Right(id))
            else (s.copy(received = id), Left(id))
          }
          .flatMap {
            case Right(id) => IO.pure(Some(id))
            case Left(id)  => log.update(_ :+ Action.Nacked(List(id))).as(None)
          }

      private def claim(id: Int): IO[Boolean] =
        state.modify { s =>
          s.batches.get(id) match {
            case Some(Status.Buffered) => (s.copy(batches = s.batches + (id -> Status.Emitted)), true)
            case _                     => (s, false)
          }
        }

      override def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[List[Int]]]]] =
        Stream.emit {
          Stream
            .awakeEvery[IO](100.millis)
            .evalMapFilter(_ => receive)
            .prefetchN(1000)
            .evalFilter(claim)
            .map { id =>
              val event = ByteBuffer.wrap(id.toString.getBytes)
              Option(LowLevelEvents(Chunk.singleton(event), List(id), None))
            }
            .onFinalize {
              state.modify(s => (s.copy(batches = Map.empty), s.batches.keys.toList.sorted)).flatMap { ids =>
                log.update(_ :+ Action.Nacked(ids)).whenA(ids.nonEmpty)
              }
            }
        }

      override def debounceCheckpoints: FiniteDuration = 1.millis

      override def releaseUnemitted: IO[Unit] =
        IO.realTime.flatMap(t => log.update(_ :+ Action.ReleaseCalled(t))) >> {
          val release =
            state
              .modify { s =>
                val (buffered, emitted) = s.batches.partition(_._2 == Status.Buffered)
                (s.copy(isOpen = false, batches = emitted), buffered.keys.toList.sorted)
              }
              .flatMap(ids => log.update(_ :+ Action.Released(ids)).whenA(ids.nonEmpty)) >>
              IO.realTime.flatMap(t => log.update(_ :+ Action.ReleaseFinished(t)))
          behaviour match {
            case ReleaseBehaviour.Normal         => release
            case ReleaseBehaviour.Slow(duration) => IO.sleep(duration) >> release
            case ReleaseBehaviour.Raise          => IO.raiseError(new RuntimeException("boom! release failed"))
          }
        }
    }

  /** Takes 1 second per batch, and 60 seconds to finalize a window */
  def slowProcessor(log: Ref[IO, Vector[Action]]): EventProcessor[IO] = { in =>
    Stream.eval(Ref[IO].of(List.empty[Unique.Token])).flatMap { tokens =>
      val middle = in.evalMap { case TokenedEvents(_, token) =>
        tokens.update(token :: _) >> IO.sleep(1.second)
      }
      val end = IO.sleep(60.seconds) >> IO.realTime.flatMap(t => log.update(_ :+ Action.ProcessorFinished(t))) >> tokens.get.map(_.reverse)
      middle.drain ++ Stream.eval(end).flatMap(Stream.emits(_))
    }
  }

  /**
   * Takes 1 second per batch. The first window emits each token as soon as it is processed, and
   * raises an error 30 seconds after its input ends. With `laterWindowsFinalize`, later windows
   * take that long to finalize, as `slowProcessor` does, so a cancelled sibling window is still
   * draining when its release begins. Without it, later windows emit each token as soon as it is
   * processed, so a cancelled sibling's drain can end at the instant it schedules the release, and
   * the supervisor can cancel the release before it begins, which `LowLevelSource` accepts.
   */
  def failingProcessor(windows: Ref[IO, Int], laterWindowsFinalize: Option[FiniteDuration]): EventProcessor[IO] = { in =>
    Stream.eval(windows.updateAndGet(_ + 1)).flatMap { window =>
      val middle = in.evalMap { case TokenedEvents(_, token) => IO.sleep(1.second).as(token) }
      if (window == 1)
        middle ++ Stream.exec(IO.sleep(30.seconds)) ++ Stream.raiseError[IO](new RuntimeException("boom! processor failed"))
      else
        laterWindowsFinalize match {
          case None => middle
          case Some(finalize) =>
            Stream.eval(Ref[IO].of(List.empty[Unique.Token])).flatMap { tokens =>
              val held = in.evalMap { case TokenedEvents(_, token) => tokens.update(token :: _) >> IO.sleep(1.second) }
              held.drain ++ Stream.eval(IO.sleep(finalize) >> tokens.get.map(_.reverse)).flatMap(Stream.emits(_))
            }
        }
    }
  }
}
