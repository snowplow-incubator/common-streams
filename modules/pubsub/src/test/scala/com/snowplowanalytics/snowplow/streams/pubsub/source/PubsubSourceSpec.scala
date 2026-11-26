/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.implicits._
import cats.effect.{Deferred, IO, Ref}
import cats.effect.kernel.Unique
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import org.specs2.Specification
import org.specs2.execute.Result

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor}
import com.snowplowanalytics.snowplow.streams.internal.{LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubSourceConfig

import PubsubSourceState.{Pulling, Released}

class PubsubSourceSpec extends Specification {
  import PubsubSourceSpec._

  def is = s2"""
  The PubsubSource, against a fake subscriber stub, should:
    nack a batch received after the release straight away $e1
    not raise an error when nacking a batch received after the release fails $e2
    end the inner stream at the release, never emit a released batch, and stop pulling, with unary pull, one pull at a time $e3
    end the inner stream at the release, never emit a released batch, and stop pulling, with unary pull, parallel pulls $e4
    with streaming pull, nack nothing on release and keep emitting $e5
    pass None through while pulling, and end the inner stream at a None once released $e6
    nack every buffered batch on release, and nothing else $e7
    raise the nack's error from the release, with the released batches already out of the state $e8
    fail the stream with a unary pull's error $e9
    fail the stream with a streaming pull's error $e10
    fail the stream with an error extending deadlines, before the release $e11
    not fail or block shutdown on an error extending deadlines after the release $e12
    report a parallel pull's error while downstream is blocked handing over an earlier batch $e13
    end the inner stream, not raise, on a background error taken after the release $e14
    on a pull error, keep extending batches still being processed, and ack them when processed $e15
    never both emit and release a batch when the release races the gate's hand-over $e16
    raise a background error taken while pulling, without waiting for an item $e17
    with unary pull and parallel pulls, start no pull after the release, and nack the batches of pulls in flight at the release $e18
  """

  def e1 = run {
    val stub = new FakeSubscriberStub(failNacks = false)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      _ <- refStates.modify(_.release)
      _ <- PubsubSource.handleReceivedMessages(config(), stub, refStates, false, Vector(FakeSubscriberStub.message("a")))
      state <- refStates.get
    } yield (stub.nacked must beEqualTo(Vector("a"))).toResult and
      (state must beEqualTo(Released(Map.empty))).toResult
  }

  def e2 = run {
    val stub = new FakeSubscriberStub(failNacks = true)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      _ <- refStates.modify(_.release)
      result <- PubsubSource.handleReceivedMessages(config(), stub, refStates, false, Vector(FakeSubscriberStub.message("a"))).attempt
    } yield (result must beRight).toResult and
      (stub.nacked must beEqualTo(Vector("a"))).toResult
  }

  // With one pull at a time, at most one pull is counted after the release. This bound holds with
  // or without the check before each pull, so this does not guard that check: e4 and e18 do.
  def e3 = run(releaseWhileStreaming(config(), maxPullsAfterRelease = 1))

  def e4 = run {
    val factor = BigDecimal(4) / Runtime.getRuntime.availableProcessors
    releaseWhileStreaming(config(parallelPullFactor = factor), maxPullsAfterRelease = 4)
  }

  def e5 = run {
    val stub = new FakeSubscriberStub(failNacks = false)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      source = PubsubSource.lowLevel(config(streamingPull = true), stub, refStates)
      // Read while the source is still running, before the outer stream's final nack
      nackedBeforeEnd <- Ref[IO].of(Vector.empty[String])
      releasedState <- Ref[IO].of(PubsubSourceState.initial)
      emitted <- source.stream
                   .take(1)
                   .flatMap { inner =>
                     inner
                       .collect { case Some(e) => e }
                       .zipWithIndex
                       .evalTap { case (_, i) =>
                         if (i == 2L) source.releaseUnemitted >> refStates.get.flatMap(releasedState.set)
                         else if (i == 5L) IO(stub.nacked).flatMap(nackedBeforeEnd.set)
                         else IO.unit
                       }
                       .map(_._1)
                       .take(6)
                   }
                   .compile
                   .toVector
      nacked <- nackedBeforeEnd.get
      state <- releasedState.get
    } yield (emitted.size must beEqualTo(6)).toResult and
      (nacked must beEmpty).toResult and
      (state.isReleased must beFalse).toResult
  }

  def e6 = run {
    val stub = new FakeSubscriberStub(failNacks = false, messageLimit = 0)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      source = PubsubSource.lowLevel(config(), stub, refStates)
      items <- source.stream
                 .take(1)
                 .flatMap { inner =>
                   inner.zipWithIndex.evalTap { case (_, i) => if (i == 2L) source.releaseUnemitted else IO.unit }.map(_._1)
                 }
                 .compile
                 .toVector
    } yield items must beEqualTo(Vector(None, None, None))
  }

  def e7 = run {
    val stub = new FakeSubscriberStub(failNacks = false)
    val cfg  = config()
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      _ <- buffer(cfg, stub, refStates, "a", "b")
      _ <- PubsubSource.lowLevel(cfg, stub, refStates).releaseUnemitted
      state <- refStates.get
    } yield (stub.nacked.sorted must beEqualTo(Vector("a", "b"))).toResult and
      (state must beEqualTo(Released(Map.empty))).toResult
  }

  def e8 = run {
    val stub = new FakeSubscriberStub(failNacks = true)
    val cfg  = config()
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      _ <- buffer(cfg, stub, refStates, "a", "b")
      result <- PubsubSource.lowLevel(cfg, stub, refStates).releaseUnemitted.attempt
      state <- refStates.get
    } yield (result.leftMap(_.getMessage) must beLeft(beEqualTo("boom! nack failed"))).toResult and
      (stub.nacked.sorted must beEqualTo(Vector("a", "b"))).toResult and
      (state must beEqualTo(Released(Map.empty))).toResult
  }

  def e9 = run(failsWith(config(), "boom! pull failed")(_.failPulls.set(true)))

  def e10 = run(failsWith(config(streamingPull = true), "boom! pull failed")(_.failPulls.set(true)))

  def e11 = run {
    val stub = new FakeSubscriberStub(failNacks = false, messageLimit = 1)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      source = PubsubSource.lowLevel(fastExtensionConfig, stub, refStates)
      result <- source.stream.flatten
                  .evalTap {
                    case Some(_) => IO(stub.failExtensions.set(true))
                    case None    => IO.unit
                  }
                  .compile
                  .drain
                  .attempt
    } yield result.leftMap(_.getMessage) must beLeft(beEqualTo("boom! extension failed"))
  }

  def e12 = run {
    val stub = new FakeSubscriberStub(failNacks = false, messageLimit = 1)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      source = PubsubSource.lowLevel(fastExtensionConfig, stub, refStates)
      result <- source.stream
                  .take(1)
                  .flatMap { inner =>
                    inner.evalTap {
                      case Some(_) => source.releaseUnemitted >> IO(stub.failExtensions.set(true))
                      case None    => IO.unit
                    } ++ Stream.exec(eventually(5.seconds)(stub.failedExtensions > 0) >> IO.sleep(100.millis))
                  }
                  .compile
                  .drain
                  .attempt
    } yield (result must beRight).toResult and
      (stub.failedExtensions must beGreaterThan(0)).toResult and
      // The emitted batch, never acked, is nacked when the outer stream is finalized
      (stub.nacked must beEqualTo(Vector("ack-1"))).toResult
  }

  def e13 = run {
    val stub   = new FakeSubscriberStub(failNacks = false)
    val factor = BigDecimal(4) / Runtime.getRuntime.availableProcessors
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      reported <- Deferred[IO, Throwable]
      fiber <- UnaryPullSource
                 .stream(config(parallelPullFactor = factor), stub, refStates, e => reported.complete(e).void)
                 .collect { case Some(e) => e }
                 .evalMap(_ => IO(stub.failPulls.set(true)) >> IO.never[Unit])
                 .compile
                 .drain
                 .start
      result <- reported.get.timeout(5.seconds).attempt
      _ <- fiber.cancel
    } yield result.leftMap(_.getMessage).map(_.getMessage) must beRight(beEqualTo("boom! pull failed"))
  }

  def e14 = run {
    for {
      handoff <- Queue.synchronous[IO, Item]
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      failed <- Deferred[IO, Throwable]
      firstEmitted <- Deferred[IO, Unit]
      fiber <- PubsubSource.gate(handoff, refStates, failed).evalTap(_ => firstEmitted.complete(()).void).compile.toVector.start
      _ <- handoff.offer(None)
      // `offer` returns once the gate takes the item, before the gate decides whether to emit it
      _ <- firstEmitted.get
      _ <- refStates.modify(_.release)
      _ <- failed.complete(new RuntimeException("boom!"))
      result <- fiber.joinWithNever.timeout(5.seconds).attempt
    } yield result must beRight(beEqualTo(Vector(None)))
  }

  def e15 = run {
    val stub = new FakeSubscriberStub(failNacks = false)
    // It keeps pulling while it processes the first batch, so the pull error reaches the stream
    // while that batch is still being processed
    val processor: EventProcessor[IO] = _.parEvalMapUnorderedUnbounded { batch =>
      if (ackIdsOf(batch.events.toVector) == Vector("ack-1"))
        IO(stub.failPulls.set(true)) >>
          eventually(5.seconds)(stub.extended.count(_ == "ack-1") >= 2).as(batch.ack)
      else
        IO.pure(batch.ack)
    }
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      sourceAndAck <- LowLevelSource.toSourceAndAck(PubsubSource.lowLevel(fastExtensionConfig, stub, refStates))
      result <- sourceAndAck.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), processor).compile.drain.attempt
    } yield (result.leftMap(_.getMessage) must beLeft(beEqualTo("boom! pull failed"))).toResult and
      // One extension when it was pulled, and at least one more while it was being processed
      (stub.extended.count(_ == "ack-1") must beGreaterThanOrEqualTo(2)).toResult and
      (stub.acked must contain("ack-1")).toResult and
      (stub.nacked must not(contain("ack-1"))).toResult
  }

  // The release and the gate's hand-over of the same batch run concurrently on the real runtime,
  // many times over. The batch must end up either emitted or released, never both and never neither.
  def e16 = {
    val iteration: IO[Boolean] =
      for {
        handoff <- Queue.synchronous[IO, Item]
        refStates <- Ref[IO].of(PubsubSourceState.initial)
        failed <- Deferred[IO, Throwable]
        token <- IO.unique
        _ <- refStates.modify(_.receive(token, PubsubBatchState(Instant.EPOCH, Vector("a"))))
        fiber <- PubsubSource.gate(handoff, refStates, failed).compile.toVector.start
        item = Option(LowLevelEvents(Chunk.empty[ByteBuffer], Vector(token), None))
        cedes <- IO(scala.util.Random.nextInt(4))
        // `offer` returns once the gate has taken the item, so the release lands around the gate's
        // hand-over, at a point that varies with the number of cedes
        released <- handoff.offer(item) >> IO.cede.replicateA_(cedes) >> refStates.modify(_.release)
        // If the gate emitted the batch, it is still running, and ends at the next item
        emitted <- IO.race(handoff.offer(None) >> fiber.joinWithNever, fiber.joinWithNever).map(_.merge)
      } yield {
        val wasEmitted  = emitted.flatten.exists(_.ack.contains(token))
        val wasReleased = released.nonEmpty
        wasEmitted != wasReleased
      }
    run(iteration.replicateA(3000).map(results => results.count(ok => !ok) must beEqualTo(0)))
  }

  def e17 = run {
    for {
      handoff <- Queue.synchronous[IO, Item]
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      failed <- Deferred[IO, Throwable]
      fiber <- PubsubSource.gate(handoff, refStates, failed).compile.toVector.start
      _ <- failed.complete(new RuntimeException("boom!"))
      result <- fiber.joinWithNever.timeout(5.seconds).attempt
      state <- refStates.get
    } yield (result.leftMap(_.getMessage) must beLeft(beEqualTo("boom!"))).toResult and
      (state must beEqualTo(Pulling(Map.empty, Map.empty))).toResult
  }

  // Every parallel pull is held open across the release, so they are all in flight at the release.
  // Once they complete, the fetch loop has free slots and keeps ticking, but must start no pull.
  def e18 = run {
    val stub   = new FakeSubscriberStub(failNacks = false)
    val factor = BigDecimal(4) / Runtime.getRuntime.availableProcessors
    stub.holdPulls.set(true)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      source = PubsubSource.lowLevel(config(parallelPullFactor = factor), stub, refStates)
      pullsLater <- Ref[IO].of(0)
      nackedLater <- Ref[IO].of(Vector.empty[String])
      fiber <- source.stream
                 .take(1)
                 .flatMap { inner =>
                   inner ++
                     // Read while the outer stream is still open, before its final nack
                     Stream.exec(
                       eventually(5.seconds)(stub.nacked.size >= 4) >>
                         untilSettled(IO(stub.pulls), 300.millis, 10.seconds).flatMap(pullsLater.set) >>
                         IO(stub.nacked).flatMap(nackedLater.set)
                     )
                 }
                 .compile
                 .toVector
                 .start
      _ <- eventually(5.seconds)(stub.held == 4)
      _ <- source.releaseUnemitted
      pullsAtRelease <- IO(stub.pulls)
      _ <- IO(stub.completeHeldPulls())
      items <- fiber.joinWithNever
      later <- pullsLater.get
      nacked <- nackedLater.get
    } yield (pullsAtRelease must beEqualTo(4)).toResult and
      (later must beEqualTo(pullsAtRelease)).toResult and
      (items.flatten must beEmpty).toResult and
      (nacked.sorted must beEqualTo(Vector("ack-1", "ack-2", "ack-3", "ack-4"))).toResult and
      (stub.acked must beEmpty).toResult
  }

  /** Receives one batch per ack ID, which the state holds as buffered */
  private def buffer(
    cfg: PubsubSourceConfig,
    stub: FakeSubscriberStub,
    refStates: Ref[IO, PubsubSourceState],
    ackIds: String*
  ): IO[Unit] =
    ackIds.toList.traverse_ { id =>
      PubsubSource.handleReceivedMessages(cfg, stub, refStates, false, Vector(FakeSubscriberStub.message(id)))
    }

  // `timeoutAndForget`, because `timeout` cannot interrupt an uncancelable region, so a hang there
  // would hang the suite instead of failing the example
  private def run(io: IO[Result]): Result =
    io.timeoutAndForget(30.seconds)
      .recover { case _: TimeoutException => failure("Did not finish within 30 seconds") }
      .unsafeRunSync()

  private def eventually(within: FiniteDuration)(cond: => Boolean): IO[Unit] = {
    lazy val loop: IO[Unit] = IO(cond).flatMap(if (_) IO.unit else IO.sleep(10.millis) >> loop)
    loop.timeoutTo(within, IO.unit)
  }

  /**
   * Waits until `read` returns the same value twice, `interval` apart, or until `within` has passed
   */
  private def untilSettled(
    read: IO[Int],
    interval: FiniteDuration,
    within: FiniteDuration
  ): IO[Int] = {
    def loop(previous: Int): IO[Int] =
      IO.sleep(interval) >> read.flatMap(current => if (current == previous) IO.pure(current) else loop(current))
    read.flatMap(loop).timeoutTo(within, read)
  }

  private def failsWith(cfg: PubsubSourceConfig, message: String)(setUp: FakeSubscriberStub => Unit): IO[Result] = {
    val stub = new FakeSubscriberStub(failNacks = false)
    setUp(stub)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      result <- PubsubSource.lowLevel(cfg, stub, refStates).stream.flatten.compile.drain.attempt
    } yield result.leftMap(_.getMessage) must beLeft(beEqualTo(message))
  }

  /**
   * Consumes 3 batches, acking each, then releases un-emitted batches, and expects the inner stream
   * to end by itself, and the fetch loop to stop pulling. The stub returns a batch as fast as it is
   * pulled, so the source's buffers are full at the release. Pulls in flight at the release may be
   * counted after it, up to `maxPullsAfterRelease`.
   */
  private def releaseWhileStreaming(cfg: PubsubSourceConfig, maxPullsAfterRelease: Int): IO[Result] = {
    val stub = new FakeSubscriberStub(failNacks = false)
    for {
      refStates <- Ref[IO].of(PubsubSourceState.initial)
      pullsAtRelease <- Ref[IO].of(0)
      pullsSettled <- Ref[IO].of(0)
      pullsLater <- Ref[IO].of(0)
      source = PubsubSource.lowLevel(cfg, stub, refStates)
      emitted <- source.stream
                   .take(1)
                   .flatMap { inner =>
                     inner
                       .collect { case Some(e) => e }
                       .evalTap(e => source.checkpointer.ack(e.ack))
                       .zipWithIndex
                       .evalTap { case (_, i) =>
                         if (i == 2L) source.releaseUnemitted >> IO(stub.pulls).flatMap(pullsAtRelease.set)
                         else IO.unit
                       }
                       .map(_._1) ++
                       // Time for the fetch loop to fill its buffers, then time to see that it has stopped
                       Stream.exec(
                         untilSettled(IO(stub.pulls), 300.millis, 10.seconds).flatMap(pullsSettled.set) >>
                           IO.sleep(300.millis) >> IO(stub.pulls).flatMap(pullsLater.set)
                       )
                   }
                   .compile
                   .toVector
      atRelease <- pullsAtRelease.get
      settled <- pullsSettled.get
      later <- pullsLater.get
    } yield {
      val emittedAckIds = emitted.flatMap(e => ackIdsOf(e.events.toVector))
      (emitted.size must beEqualTo(3)).toResult and
        (stub.acked must beEqualTo(emittedAckIds)).toResult and
        (stub.nacked must not(beEmpty)).toResult and
        (stub.acked.toSet.intersect(stub.nacked.toSet) must beEmpty).toResult and
        // Compared while the outer stream is still open, before the fetch loop is cancelled
        (later must beEqualTo(settled)).toResult and
        (later - atRelease must beLessThanOrEqualTo(maxPullsAfterRelease)).toResult
    }
  }
}

object PubsubSourceSpec {

  type Item = Option[LowLevelEvents[Vector[Unique.Token]]]

  def config(
    parallelPullFactor: BigDecimal          = BigDecimal("0.0001"),
    streamingPull: Boolean                  = false,
    durationPerAckExtension: FiniteDuration = 60.seconds,
    minRemainingAckDeadline: BigDecimal     = BigDecimal("0.1")
  ): PubsubSourceConfig =
    PubsubSourceConfig(
      subscription            = PubsubSourceConfig.Subscription("my-project", "my-subscription"),
      parallelPullFactor      = parallelPullFactor,
      durationPerAckExtension = durationPerAckExtension,
      minRemainingAckDeadline = minRemainingAckDeadline,
      maxMessagesPerPull      = 1,
      debounceRequests        = 1.millis,
      streamingPull           = streamingPull,
      retries                 = PubsubSourceConfig.Retries(PubsubSourceConfig.TransientErrorRetrying(10.millis, 1))
    )

  /** Extends a batch's deadline within about half a second of receiving it */
  val fastExtensionConfig: PubsubSourceConfig =
    config(durationPerAckExtension = 1.second, minRemainingAckDeadline = BigDecimal("0.9"))

  private def ackIdsOf(events: Vector[ByteBuffer]): Vector[String] =
    events.map(b => StandardCharsets.UTF_8.decode(b.duplicate()).toString)
}
