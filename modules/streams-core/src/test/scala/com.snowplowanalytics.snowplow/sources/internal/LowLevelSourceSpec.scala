/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.internal

import cats.effect.IO
import cats.effect.kernel.{Ref, Unique}
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import fs2.{Chunk, Stream}
import org.specs2.Specification
import org.specs2.matcher.Matcher

import scala.concurrent.duration.{Duration, DurationInt}

import java.nio.charset.StandardCharsets
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}

import java.nio.ByteBuffer

class LowLevelSourceSpec extends Specification with CatsEffect {
  import LowLevelSourceSpec._

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck should:
    With no windowing of events:
      process and checkpoint a continuous stream of events with no windowing $e1
      cleanly checkpoint pending events when a stream is interrupted $e2
      not checkpoint events if the event processor throws an exception $e3

    With a processor that operates on windows of events:
      process and checkpoint events in timed windows $e4
      cleanly checkpoint pending window when a stream is interrupted $e5
      not checkpoint events if the event processor throws an exception $e6

    When reporting healthy status
      report healthy when there are no events $e7
      report lagging if there are unprocessed events $e8
      report healthy if events are processed but not yet acked (e.g. a batch-oriented loader) $e9
      report healthy after all events have been processed and acked $e10
      report disconnected while source is in between two active streams of events (e.g. during kafka rebalance) $e11
  """

  def e1 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    val numBatchesToTest  = (2.5 * BatchesPerRebalance).toInt // Enough to test two full rebalances
    val durationToTest    = numBatchesToTest * TimeBetweenBatches
    val expectedNumEvents = numBatchesToTest * EventsPerBatch
    val expected          = pureEvents.take(expectedNumEvents.toLong).compile.toList

    val io = for {
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      refProcessed <- Ref[IO].of[List[String]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      processor = testProcessor(refProcessed)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(durationToTest)
      checkpointed <- refCheckpoints.get
      processed <- refProcessed.get
      _ <- fiber.cancel
    } yield (checkpointed must haveSize(numBatchesToTest)) and
      (checkpointed must eachHaveSize(EventsPerBatch)) and
      (checkpointed.flatten must beEqualTo(processed)) and
      (processed must haveSize(expectedNumEvents)) and
      (processed must beSorted) and
      (processed must containUniqueStrings) and
      (processed must beEqualTo(expected))

    TestControl.executeEmbed(io)
  }

  def e2 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    val durationToTest    = 0.5 * TimeToProcessBatch // Not enough time to finish processing the first batch
    val expectedNumEvents = 1 * EventsPerBatch // Because the first batch should be allowed to finish
    val expected          = pureEvents.take(expectedNumEvents.toLong).compile.toList

    val io = for {
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      refProcessed <- Ref[IO].of[List[String]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      processor = testProcessor(refProcessed)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(durationToTest) // Not enough time to finish processing the first batch
      _ <- fiber.cancel // This should wait for the first batch to finish processing
      checkpointed <- refCheckpoints.get
      processed <- refProcessed.get
    } yield (checkpointed must haveSize(1)) and
      (checkpointed.head must haveSize(expectedNumEvents)) and
      (checkpointed.flatten must beEqualTo(processed)) and
      (processed must haveSize(expectedNumEvents)) and
      (processed must beEqualTo(expected))

    // TODO: check it is cancelled in reasonable time
    TestControl.executeEmbed(io)
  }

  def e3 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    val errorAfterBatch   = 3
    val expectedNumEvents = 3 * EventsPerBatch
    val expected          = pureEvents.take(expectedNumEvents.toLong).compile.toList

    def badProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
      _.zipWithIndex
        .evalMap { case (TokenedEvents(events, token, _), batchId) =>
          if (batchId >= errorAfterBatch)
            IO.raiseError(new RuntimeException(s"boom! Exceeded $errorAfterBatch batches"))
          else
            ref
              .update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString).toList)
              .as(token)
        }

    val io = for {
      refProcessed <- Ref[IO].of[List[String]](Nil)
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      processor = badProcessor(refProcessed)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      result <- sourceAndAck.stream(config, processor).compile.drain.attempt
      checkpointed <- refCheckpoints.get
      processed <- refProcessed.get
    } yield (result must beLeft) and
      (checkpointed must haveSize(errorAfterBatch)) and
      (checkpointed must eachHaveSize(EventsPerBatch)) and
      (checkpointed.flatten must beEqualTo(processed)) and
      (processed must haveSize(expectedNumEvents)) and
      (processed must beEqualTo(expected))

    TestControl.executeEmbed(io)
  }

  def e4 = {

    val windowDuration =
      (BatchesPerRebalance - 1) * TimeBetweenBatches - 1.milliseconds // so for each rebalance we get 1 full and 1 incomplete window
    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(windowDuration, 1.0))

    val durationToTest =
      (2 * BatchesPerRebalance + 1) * TimeBetweenBatches // so no time to process first window of the 3rd rebalance
    val expectedNumEvents  = 2 * BatchesPerRebalance * EventsPerBatch
    val expectedNumWindows = 4
    val expected           = pureEvents.take(expectedNumEvents.toLong).compile.toList

    val io = for {
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      refProcessed <- Ref[IO].of[List[String]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      processor = windowedProcessor(refProcessed)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(durationToTest)
      checkpointed <- refCheckpoints.get
      _ <- fiber.cancel
    } yield (checkpointed must haveSize(expectedNumWindows)) and
      (checkpointed.head must haveSize((BatchesPerRebalance - 1) * EventsPerBatch)) and
      (checkpointed.flatten must haveSize(expectedNumEvents)) and
      (checkpointed.flatten must beSorted) and
      (checkpointed.flatten must containUniqueStrings) and
      (checkpointed.flatten must beEqualTo(expected))

    TestControl.executeEmbed(io)
  }

  def e5 = {

    val windowDuration =
      (BatchesPerRebalance - 1) * TimeBetweenBatches - 1.milliseconds // so for each rebalance we get 1 full and 1 incomplete window
    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(windowDuration, 1.0))

    val durationToTest    = 1.5 * TimeBetweenBatches // Not enough time to finish an entire window
    val expectedNumEvents = 2 * EventsPerBatch // Because the first two batches should be allowed to finish
    val expected          = pureEvents.take(expectedNumEvents.toLong).compile.toList

    val io = for {
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      refProcessed <- Ref[IO].of[List[String]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      processor = windowedProcessor(refProcessed)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(durationToTest) // Not enough time to finish processing the first batch
      _ <- fiber.cancel // This should wait for the first batch to finish processing
      checkpointed <- refCheckpoints.get
      processed <- refProcessed.get
    } yield (checkpointed must haveSize(1)) and
      (checkpointed.head must haveSize(expectedNumEvents)) and
      (checkpointed.flatten must beEqualTo(processed)) and
      (processed must haveSize(expectedNumEvents)) and
      (processed must beEqualTo(expected))

    // TODO: check it is cancelled in reasonable time
    TestControl.executeEmbed(io)
  }

  def e6 = {

    val windowDuration = 3 * TimeBetweenBatches
    val config         = EventProcessingConfig(EventProcessingConfig.TimedWindows(windowDuration, 1.0))

    val badProcessor: EventProcessor[IO] =
      _.drain ++ Stream.raiseError[IO](new RuntimeException("boom!"))

    val io = for {
      refCheckpoints <- Ref[IO].of[List[List[String]]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refCheckpoints))
      result <- sourceAndAck.stream(config, badProcessor).compile.drain.attempt
      checkpointed <- refCheckpoints.get
    } yield (result must beLeft) and
      (checkpointed must beEmpty)

    TestControl.executeEmbed(io)
  }

  def e7 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    // A source that emits nothing
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                 = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, LowLevelEvents[Unit]]] = Stream.emit(Stream.never[IO])
    }

    val io = for {
      refProcessed <- Ref[IO].of[List[String]](Nil)
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refProcessed)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      health <- sourceAndAck.isHealthy(Duration.Zero)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def e8 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    // A source that repeatedly emits a batch
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit] = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, LowLevelEvents[Unit]]] = Stream.emit {
        Stream.emit(LowLevelEvents(Chunk.empty, (), None)).repeat
      }
    }

    // A processor which takes 1 hour to process each batch
    val processor: EventProcessor[IO] =
      _.evalMap { case TokenedEvents(_, token, _) =>
        IO.sleep(1.hour).as(token)
      }

    val io = for {
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      health <- sourceAndAck.isHealthy(10.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.LaggingEventProcessor(5.minutes))

    TestControl.executeEmbed(io)
  }

  def e9 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    // A source that repeatedly emits a batch
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit] = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, LowLevelEvents[Unit]]] = Stream.emit {
        Stream.emit(LowLevelEvents(Chunk.empty, (), None)).repeat
      }
    }

    // A processor which takes 1 minute to process each batch, but does not emit the token (i.e. does not ack the batch)
    val processor: EventProcessor[IO] =
      _.evalMap(_ => IO.sleep(1.minute)).drain.covaryOutput

    val io = for {
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(342.seconds)
      health <- sourceAndAck.isHealthy(90.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def e10 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    // A source that emits one batch and then nothing forevermore
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit] = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, LowLevelEvents[Unit]]] = Stream.emit {
        Stream.emit(LowLevelEvents(Chunk.empty, (), None)) ++ Stream.never[IO]
      }
    }

    // A processor which takes 1 minute to process each batch
    val processor: EventProcessor[IO] =
      _.evalMap { case TokenedEvents(_, token, _) =>
        IO.sleep(1.minute).as(token)
      }

    val io = for {
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      health <- sourceAndAck.isHealthy(1.microsecond)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def e11 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing)

    // A source that emits one batch per inner stream, with a 5 minute "rebalancing" in between
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit] = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, LowLevelEvents[Unit]]] =
        Stream.fixedDelay[IO](5.minutes).map { _ =>
          Stream.emit(LowLevelEvents(Chunk.empty, (), None))
        }
    }

    // A processor which takes 5 seconds to process each batch
    val processor: EventProcessor[IO] =
      _.evalMap { case TokenedEvents(_, token, _) =>
        IO.sleep(5.seconds).as(token)
      }

    val io = for {
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(2.minutes)
      health <- sourceAndAck.isHealthy(1.microsecond)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Disconnected)

    TestControl.executeEmbed(io)
  }

  def containUniqueStrings: Matcher[Seq[String]] = { (items: Seq[String]) =>
    (items.toSet.size == items.size, s"$items contains non-unique values")
  }

  def eachHaveSize(expected: Int): Matcher[Seq[Seq[String]]] = { (items: Seq[Seq[String]]) =>
    (items.forall(_.size == expected), s"$items contains items that do not have length $expected")
  }
}

object LowLevelSourceSpec {

  val EventsPerBatch      = 8
  val BatchesPerRebalance = 5
  val TimeBetweenBatches  = 20.seconds
  val TimeToProcessBatch  = 1.second

  /**
   * An EventProcessor which:
   *
   *   - Records what events it received
   *   - Emits the checkpointing tokens immediately
   */
  def testProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token, _) =>
      for {
        _ <- IO.sleep(TimeToProcessBatch)
        _ <- ref.update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString).toList)
      } yield token
    }

  /**
   * An EventProcessor which:
   *
   *   - Records what events it received
   *   - Delays emitting the checkpointing tokens until the end of the window
   */
  def windowedProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] = { in =>
    Stream.eval(Ref[IO].of[List[Unique.Token]](Nil)).flatMap { checkpoints =>
      val out = in.evalMap { case TokenedEvents(events, token, _) =>
        for {
          _ <- IO.sleep(TimeToProcessBatch)
          _ <- ref.update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString).toList)
          _ <- checkpoints.update(token :: _)
        } yield ()
      }
      out.drain ++ Stream.eval(checkpoints.get).flatMap(cs => Stream.emits(cs.reverse))
    }
  }

  /**
   * A LowLevelSource with these features:
   *
   *   - It emits batches of events at regular intervals
   *   - It "rebalances" (like Kafka) after every few batches, which means it emits a new stream
   *   - It uses a ref to record which events got checkpointed
   */
  def testLowLevelSource(ref: Ref[IO, List[List[String]]]): LowLevelSource[IO, List[String]] =
    new LowLevelSource[IO, List[String]] {
      def checkpointer: Checkpointer[IO, List[String]] = Checkpointer.acksOnly[IO, List[String]] { toCheckpoint =>
        ref.update(_ :+ toCheckpoint)
      }

      def stream: Stream[IO, Stream[IO, LowLevelEvents[List[String]]]] =
        Stream.range(1, Int.MaxValue).map { rebalanceId =>
          Stream.range(1, BatchesPerRebalance + 1).flatMap { batchId =>
            val events = (1 to EventsPerBatch)
              .map(eventId => s"rebalance $rebalanceId - batch $batchId - event $eventId")
            val asBytes = Chunk
              .from(events)
              .map(_.getBytes(StandardCharsets.UTF_8))
              .map(ByteBuffer.wrap)
            Stream.emit(LowLevelEvents(events = asBytes, ack = events.toList, earliestSourceTstamp = None)) ++ Stream
              .sleep[IO](TimeBetweenBatches)
              .drain
          }
        }
    }

  def pureEvents: Stream[fs2.Pure, String] =
    Stream.range(1, Int.MaxValue).flatMap { rebalanceId =>
      Stream.iterable {
        for {
          batchId <- 1 to BatchesPerRebalance
          eventId <- 1 to EventsPerBatch
        } yield s"rebalance $rebalanceId - batch $batchId - event $eventId"
      }
    }

}
