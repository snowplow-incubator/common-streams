/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.implicits._
import cats.effect.std.Queue
import cats.effect.IO
import cats.effect.kernel.{Ref, Unique}
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import fs2.{Chunk, Stream}
import org.specs2.Specification

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import java.nio.charset.StandardCharsets
import java.time.Instant

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}

import java.nio.ByteBuffer

class LowLevelSourceSpec extends Specification with CatsEffect {
  import LowLevelSourceSpec._

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck should:
    With no windowing of events:
      process and checkpoint a continuous stream of events with no windowing:
        - when time between batches is longer than the time to process a batch $e1
        - when time between batches is shorter than the time to process a batch $e2
      not checkpoint events if the event processor throws an exception $e3
      delay checkpoints according to the debounceCheckpoints config $e4

    With a processor that operates on windows of events:
      process and checkpoint events in timed windows $windowed1
      cleanly checkpoint pending window when a stream is interrupted $windowed2
      not checkpoint events if the event processor throws an exception $windowed3
      eagerly start windows when previous window is still finalizing $windowed4
      use a short first window according to the configuration $windowed5
      not delay checkpoints at the end of a window when debounceCheckpoints is large $windowed6

    When reporting healthy status
      report healthy when there are no events but the source emits periodic liveness pings $health1
      report unhealthy when there are no events and no liveness pings $health2
      report lagging if there are unprocessed events $health3
      report healthy if events are processed but not yet acked (e.g. a batch-oriented loader) $health4
      report healthy after all events have been processed and acked $health5
      report disconnected while source is in between two active streams of events (e.g. during kafka rebalance) $health6
      report unhealthy if the underlying low level source is lagging $health7

    When reporting currentStreamLatency
      report no timestamp when there are no events $latency1
      report a timestamp if there are unprocessed events $latency2
      report no timestamp after events are processed $latency3

    When pushing latency metrics
      report no metric when there are no events and no liveness pings $latencyMetric1
      report zero latency when there are no events, but regular liveness pings $latencyMetric2
      report non-zero latency when the source emits timestamped events $latencyMetric3
  """

  def e1 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(235.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.Checkpointed(List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.Checkpointed(List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.Checkpointed(List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:00Z", List("25", "26", "27", "28", "29", "30", "31", "32")),
        Action.Checkpointed(List("25", "26", "27", "28", "29", "30", "31", "32")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:20Z", List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.Checkpointed(List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:40Z", List("41", "42", "43", "44", "45", "46", "47", "48")),
        Action.Checkpointed(List("41", "42", "43", "44", "45", "46", "47", "48")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:00Z", List("49", "50", "51", "52", "53", "54", "55", "56")),
        Action.Checkpointed(List("49", "50", "51", "52", "53", "54", "55", "56")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:20Z", List("57", "58", "59", "60", "61", "62", "63", "64")),
        Action.Checkpointed(List("57", "58", "59", "60", "61", "62", "63", "64")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:40Z", List("65", "66", "67", "68", "69", "70", "71", "72")),
        Action.Checkpointed(List("65", "66", "67", "68", "69", "70", "71", "72")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:00Z", List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.Checkpointed(List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:20Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:03:20Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:20Z", List("81", "82", "83", "84", "85", "86", "87", "88")),
        Action.Checkpointed(List("81", "82", "83", "84", "85", "86", "87", "88")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:40Z", List("89", "90", "91", "92", "93", "94", "95", "96")),
        Action.Checkpointed(List("89", "90", "91", "92", "93", "94", "95", "96")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:55Z")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e2 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 100,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 1.seconds, // source is quick to emit more events
      timeToProcessBatch  = 60.second // processor is slow to sink the events
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(90.seconds) // processor will be mid-way through processing second batch
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:00Z", List("3", "4")),
        Action.Checkpointed(List("1", "2")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:00Z", List("5", "6")),
        Action.Checkpointed(List("3", "4")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:00Z"),
        Action.Checkpointed(List("5", "6"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def e3 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second
    )

    // A processor that throws an error after the 3rd batch it sees
    def badProcessor(inner: EventProcessor[IO]): EventProcessor[IO] = { in =>
      Stream.eval(Queue.synchronous[IO, TokenedEvents]).flatMap { q =>
        val str = in.zipWithIndex
          .evalMap { case (tokenedEvents, batchId) =>
            if (batchId >= 3)
              IO.raiseError(new RuntimeException(s"boom! Exceeded 3 batches"))
            else
              q.offer(tokenedEvents)
          }
        inner(Stream.fromQueueUnterminated(q)).concurrently(str)
      }
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      innerProcessor = testProcessor(refActions, testConfig)
      processor      = badProcessor(innerProcessor)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(2.days)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.Checkpointed(List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.Checkpointed(List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.Checkpointed(List("17", "18", "19", "20", "21", "22", "23", "24"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def e4 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second,
      debounceCheckpoints = 30.seconds
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(235.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.Checkpointed(List("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:00Z", List("25", "26", "27", "28", "29", "30", "31", "32")),
        Action.Checkpointed(List("17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:20Z", List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:40Z"),
        Action.Checkpointed(List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorStartedWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:40Z", List("41", "42", "43", "44", "45", "46", "47", "48")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:00Z", List("49", "50", "51", "52", "53", "54", "55", "56")),
        Action.Checkpointed(List("41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:20Z", List("57", "58", "59", "60", "61", "62", "63", "64")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:40Z", List("65", "66", "67", "68", "69", "70", "71", "72")),
        Action.Checkpointed(List("57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:00Z", List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:20Z"),
        Action.Checkpointed(List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorStartedWindow("1970-01-01T00:03:20Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:20Z", List("81", "82", "83", "84", "85", "86", "87", "88")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:40Z", List("89", "90", "91", "92", "93", "94", "95", "96")),
        Action.Checkpointed(List("81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:55Z")
      )
    )

    TestControl.executeEmbed(io)
  }

  /** Specs for when the processor works with windows */

  def windowed1 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(45.seconds, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(235.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:45Z"),
        Action.Checkpointed(
          List(
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24"
          )
        ),
        Action.ProcessorStartedWindow("1970-01-01T00:01:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:00Z", List("25", "26", "27", "28", "29", "30", "31", "32")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:20Z", List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorStartedWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:40Z", List("41", "42", "43", "44", "45", "46", "47", "48")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:40Z"),
        Action.Checkpointed(List("25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:00Z", List("49", "50", "51", "52", "53", "54", "55", "56")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:20Z", List("57", "58", "59", "60", "61", "62", "63", "64")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:02:25Z"),
        Action.Checkpointed(
          List(
            "41",
            "42",
            "43",
            "44",
            "45",
            "46",
            "47",
            "48",
            "49",
            "50",
            "51",
            "52",
            "53",
            "54",
            "55",
            "56",
            "57",
            "58",
            "59",
            "60",
            "61",
            "62",
            "63",
            "64"
          )
        ),
        Action.ProcessorStartedWindow("1970-01-01T00:02:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:40Z", List("65", "66", "67", "68", "69", "70", "71", "72")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:00Z", List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorStartedWindow("1970-01-01T00:03:20Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:20Z", List("81", "82", "83", "84", "85", "86", "87", "88")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:20Z"),
        Action.Checkpointed(List("65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:40Z", List("89", "90", "91", "92", "93", "94", "95", "96")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:55Z"),
        Action.Checkpointed(List("81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def windowed2 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(45.seconds, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 5,
      timeBetweenBatches  = 0.seconds,
      timeToProcessBatch  = 21.seconds // Time to process a batch is fairly slow
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(30.seconds) // Mid-way through processing second batch
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:21Z", List("6", "7", "8", "9", "10")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:42Z", List("11", "12", "13", "14", "15")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:03Z"),
        Action.Checkpointed(List("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def windowed3 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(10.minutes, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second
    )

    // A processor that throws an error after the 3rd batch it sees
    def badProcessor(inner: EventProcessor[IO]): EventProcessor[IO] = { in =>
      Stream.eval(Queue.synchronous[IO, TokenedEvents]).flatMap { q =>
        val str = in.zipWithIndex
          .evalMap { case (tokenedEvents, batchId) =>
            if (batchId >= 3)
              IO.raiseError(new RuntimeException(s"boom! Exceeded 3 batches"))
            else
              q.offer(tokenedEvents)
          }
        inner(Stream.fromQueueUnterminated(q)).concurrently(str)
      }
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      innerProcessor = windowedProcessor(refActions, testConfig)
      processor      = badProcessor(innerProcessor)
      _ <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(2.days)
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def windowed4 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(10.seconds, 1.0, numEagerWindows = 4), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance  = Int.MaxValue,
      eventsPerBatch       = 2,
      timeBetweenBatches   = 8.seconds,
      timeToProcessBatch   = 1.second,
      timeToFinalizeWindow = 90.seconds
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(118.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:08Z", List("3", "4")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:10Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:00:16Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:16Z", List("5", "6")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:24Z", List("7", "8")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:26Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:00:32Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:32Z", List("9", "10")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("11", "12")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:42Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:00:48Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:48Z", List("13", "14")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:56Z", List("15", "16")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:58Z"),
        Action.ProcessorStartedWindow("1970-01-01T00:01:04Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:04Z", List("17", "18")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:12Z", List("19", "20")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:14Z"),
        Action.Checkpointed(List("1", "2", "3", "4")),
        Action.ProcessorStartedWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:40Z", List("21", "22")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:48Z", List("23", "24")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:50Z"),
        Action.Checkpointed(List("5", "6", "7", "8")),
        Action.ProcessorStartedWindow("1970-01-01T00:01:56Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:56Z", List("25", "26")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:58Z"),
        Action.Checkpointed(List("9", "10", "11", "12")),
        Action.Checkpointed(List("13", "14", "15", "16")),
        Action.Checkpointed(List("17", "18", "19", "20")),
        Action.Checkpointed(List("21", "22", "23", "24")),
        Action.Checkpointed(List("25", "26"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def windowed5 = {

    val config = EventProcessingConfig(
      EventProcessingConfig.TimedWindows(
        duration           = 60.seconds,
        firstWindowScaling = 0.25, // so first window is 15 seconds
        numEagerWindows    = 2
      ),
      _ => IO.unit
    )

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 11.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(131.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:11Z", List("3", "4")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:15Z"),
        Action.Checkpointed(List("1", "2", "3", "4")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:22Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:22Z", List("5", "6")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:33Z", List("7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:44Z", List("9", "10")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:52Z"),
        Action.Checkpointed(List("5", "6", "7", "8", "9", "10")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:55Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:55Z", List("11", "12")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:06Z", List("13", "14")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:17Z", List("15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:28Z", List("17", "18")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:39Z", List("19", "20")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:50Z", List("21", "22")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:55Z"),
        Action.Checkpointed(List("11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22")),
        Action.ProcessorStartedWindow("1970-01-01T00:02:01Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:01Z", List("23", "24")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:02:11Z"),
        Action.Checkpointed(List("23", "24"))
      )
    )

    TestControl.executeEmbed(io)
  }

  def windowed6 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(45.seconds, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 5,
      eventsPerBatch      = 8,
      timeBetweenBatches  = 20.seconds,
      timeToProcessBatch  = 1.second,
      debounceCheckpoints = 30.hours
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(235.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2", "3", "4", "5", "6", "7", "8")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:20Z", List("9", "10", "11", "12", "13", "14", "15", "16")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:40Z", List("17", "18", "19", "20", "21", "22", "23", "24")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:45Z"),
        Action.Checkpointed(
          List(
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24"
          )
        ),
        Action.ProcessorStartedWindow("1970-01-01T00:01:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:00Z", List("25", "26", "27", "28", "29", "30", "31", "32")),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:20Z", List("33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorStartedWindow("1970-01-01T00:01:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:01:40Z", List("41", "42", "43", "44", "45", "46", "47", "48")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:01:40Z"),
        Action.Checkpointed(List("25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:00Z", List("49", "50", "51", "52", "53", "54", "55", "56")),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:20Z", List("57", "58", "59", "60", "61", "62", "63", "64")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:02:25Z"),
        Action.Checkpointed(
          List(
            "41",
            "42",
            "43",
            "44",
            "45",
            "46",
            "47",
            "48",
            "49",
            "50",
            "51",
            "52",
            "53",
            "54",
            "55",
            "56",
            "57",
            "58",
            "59",
            "60",
            "61",
            "62",
            "63",
            "64"
          )
        ),
        Action.ProcessorStartedWindow("1970-01-01T00:02:40Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:02:40Z", List("65", "66", "67", "68", "69", "70", "71", "72")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:00Z", List("73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorStartedWindow("1970-01-01T00:03:20Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:20Z", List("81", "82", "83", "84", "85", "86", "87", "88")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:20Z"),
        Action.Checkpointed(List("65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80")),
        Action.ProcessorReceivedEvents("1970-01-01T00:03:40Z", List("89", "90", "91", "92", "93", "94", "95", "96")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:03:55Z"),
        Action.Checkpointed(List("81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96"))
      )
    )

    TestControl.executeEmbed(io)
  }

  /** Specs for health check */

  def health1 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    // A source that emits periodic liveness pings
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.awakeDelay[IO](1.second).map(_ => None))
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      health <- sourceAndAck.isHealthy(10.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def health2 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    // A source that emits nothing
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.never[IO])
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      health <- sourceAndAck.isHealthy(1.nanosecond)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.InactiveSource(1.hour))

    TestControl.executeEmbed(io)
  }

  def health3 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 0.second,
      timeToProcessBatch  = 1.hour // Processor is very slow to sink the events
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      health <- sourceAndAck.isHealthy(10.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.LaggingEventProcessor(5.minutes))

    TestControl.executeEmbed(io)
  }

  def health4 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(1.hour, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 1.second,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(30.minutes)
      health <- sourceAndAck.isHealthy(5.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def health5 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = 1,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 100000.days, // Near-infinite pause after emitting the first batch
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      health <- sourceAndAck.isHealthy(2.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Healthy)

    TestControl.executeEmbed(io)
  }

  def health6 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    // A source that emits one batch per inner stream, with a 5 minute "rebalancing" in between
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit] = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] =
        Stream.fixedDelay[IO](5.minutes).map { _ =>
          Stream.emit(Some(LowLevelEvents(Chunk.empty, (), None)))
        }
      def debounceCheckpoints: FiniteDuration = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(2.minutes)
      health <- sourceAndAck.isHealthy(1.microsecond)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.Disconnected)

    TestControl.executeEmbed(io)
  }

  def health7 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.never[IO])
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refProcessed <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refProcessed, TestSourceConfig(1, 1, 1.second, 1.second))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      health <- sourceAndAck.isHealthy(5.seconds)
      _ <- fiber.cancel
    } yield health must beEqualTo(SourceAndAck.InactiveSource(5.minutes))

    TestControl.executeEmbed(io)
  }

  /** Specs for currentStreamLatency */

  def latency1 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    // A source that emits nothing
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.never[IO])
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      reportedLatency <- sourceAndAck.currentStreamLatency
      _ <- fiber.cancel
    } yield reportedLatency must beNone

    TestControl.executeEmbed(io)
  }

  def latency2 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val streamTstamp = Instant.parse("2024-01-02T03:04:05.123Z")
    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 0.second,
      timeToProcessBatch  = 1.hour, // Processor is very slow to sink the events
      streamTstamp        = streamTstamp
    )

    val io = for {
      _ <- IO.sleep(streamTstamp.toEpochMilli.millis + 2.minutes)
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = testProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(5.minutes)
      reportedLatency <- sourceAndAck.currentStreamLatency
      _ <- fiber.cancel
    } yield reportedLatency must beSome(7.minutes)

    TestControl.executeEmbed(io)
  }

  def latency3 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(1.hour, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 1.second,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(30.minutes)
      reportedLatency <- sourceAndAck.currentStreamLatency
      _ <- fiber.cancel
    } yield reportedLatency must beNone

    TestControl.executeEmbed(io)
  }

  /** Specs for latency metric */

  def latencyMetric1 = {

    // A source that emits nothing
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.never[IO])
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      refLatencies <- Ref[IO].of(Vector.empty[FiniteDuration])
      config = EventProcessingConfig(EventProcessingConfig.NoWindowing, metric => refLatencies.update(_ :+ metric))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      latencyMetrics <- refLatencies.get
      _ <- fiber.cancel
    } yield latencyMetrics must beEmpty

    TestControl.executeEmbed(io)
  }

  def latencyMetric2 = {

    // A source that emits periodic liveness pings
    val lowLevelSource = new LowLevelSource[IO, Unit] {
      def checkpointer: Checkpointer[IO, Unit]                         = Checkpointer.acksOnly[IO, Unit](_ => IO.unit)
      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[Unit]]]] = Stream.emit(Stream.awakeDelay[IO](1.second).map(_ => None))
      def debounceCheckpoints: FiniteDuration                          = 42.seconds
    }

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(lowLevelSource)
      processor = testProcessor(refActions, TestSourceConfig(1, 1, 1.second, 1.second))
      refLatencies <- Ref[IO].of(Vector.empty[FiniteDuration])
      config = EventProcessingConfig(EventProcessingConfig.NoWindowing, metric => refLatencies.update(_ :+ metric))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(1.hour)
      latencyMetrics <- refLatencies.get
      _ <- fiber.cancel
    } yield latencyMetrics.toSet must contain(exactly(Duration.Zero))

    TestControl.executeEmbed(io)
  }

  def latencyMetric3 = {

    val streamTstamp = Instant.parse("2024-01-02T03:04:05.123Z")
    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 1.second,
      timeToProcessBatch  = 1.second,
      streamTstamp        = streamTstamp
    )

    val io = for {
      _ <- IO.sleep(streamTstamp.toEpochMilli.millis + 2.minutes)
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      refLatencies <- Ref[IO].of(Vector.empty[FiniteDuration])
      config = EventProcessingConfig(EventProcessingConfig.NoWindowing, metric => refLatencies.update(_ :+ metric))
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(10.seconds)
      latencyMetrics <- refLatencies.get
      _ <- fiber.cancel
    } yield latencyMetrics must contain(beBetween(120.seconds, 130.seconds))

    TestControl.executeEmbed(io)
  }

}

object LowLevelSourceSpec {

  sealed trait Action

  object Action {
    case class ProcessorStartedWindow(timestamp: String) extends Action
    case class ProcessorReachedEndOfWindow(timestamp: String) extends Action
    case class ProcessorReceivedEvents(timestamp: String, events: List[String]) extends Action
    case class Checkpointed(events: List[String]) extends Action
  }

  case class TestSourceConfig(
    batchesPerRebalance: Int,
    eventsPerBatch: Int,
    timeBetweenBatches: FiniteDuration,
    timeToProcessBatch: FiniteDuration,
    timeToFinalizeWindow: FiniteDuration = 0.seconds,
    streamTstamp: Instant                = Instant.EPOCH,
    debounceCheckpoints: FiniteDuration  = 1.millis
  )

  /**
   * An EventProcessor which:
   *
   *   - Records what events it received
   *   - Emits the checkpointing tokens immediately
   */
  def testProcessor(ref: Ref[IO, Vector[Action]], config: TestSourceConfig): EventProcessor[IO] = { in =>
    val start = IO.realTimeInstant.flatMap(t => ref.update(_ :+ Action.ProcessorStartedWindow(t.toString)))
    val end   = IO.realTimeInstant.flatMap(t => ref.update(_ :+ Action.ProcessorReachedEndOfWindow(t.toString)))

    val middle = in.evalMap { case TokenedEvents(events, token) =>
      val deserialized = events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString).toList
      for {
        now <- IO.realTimeInstant
        _ <- ref.update(_ :+ Action.ProcessorReceivedEvents(now.toString, deserialized))
        _ <- IO.sleep(config.timeToProcessBatch)
      } yield token
    }

    Stream.eval(start).drain ++ middle ++ Stream.eval(end).drain

  }

  /**
   * An EventProcessor which:
   *
   *   - Records what events it received
   *   - Delays emitting the checkpointing tokens until the end of the window
   */
  def windowedProcessor(ref: Ref[IO, Vector[Action]], config: TestSourceConfig): EventProcessor[IO] = { in =>
    Stream.eval(Ref[IO].of[List[Unique.Token]](Nil)).flatMap { checkpoints =>
      val start = IO.realTimeInstant.flatMap(t => ref.update(_ :+ Action.ProcessorStartedWindow(t.toString)))
      val end = for {
        t <- IO.realTimeInstant
        _ <- IO.sleep(1.nanos) // Forces deterministic order of Actions
        _ <- ref.update(_ :+ Action.ProcessorReachedEndOfWindow(t.toString))
        _ <- IO.sleep(config.timeToFinalizeWindow)
        tokens <- checkpoints.get
      } yield tokens.reverse

      val middle = in.evalMap { case TokenedEvents(events, token) =>
        val deserialized = events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString).toList
        for {
          now <- IO.realTimeInstant
          _ <- ref.update(_ :+ Action.ProcessorReceivedEvents(now.toString, deserialized))
          _ <- checkpoints.update(token :: _)
          _ <- IO.sleep(config.timeToProcessBatch)
        } yield ()
      }

      Stream.eval(start).drain ++ middle.drain ++ Stream.eval(end).flatMap(Stream.emits(_))
    }
  }

  /**
   * A LowLevelSource with these features:
   *
   *   - It emits batches of events at regular intervals
   *   - It "rebalances" (like Kafka) after every few batches, which means it emits a new stream
   *   - It periodically emits `None`s to report its liveness
   *   - It uses a ref to record which events got checkpointed
   */
  def testLowLevelSource(ref: Ref[IO, Vector[Action]], config: TestSourceConfig): LowLevelSource[IO, List[String]] =
    new LowLevelSource[IO, List[String]] {
      def checkpointer: Checkpointer[IO, List[String]] = Checkpointer.acksOnly[IO, List[String]] { toCheckpoint =>
        IO.sleep(1.nanos) *>
          ref.update(_ :+ Action.Checkpointed(toCheckpoint))
      }

      def stream: Stream[IO, Stream[IO, Option[LowLevelEvents[List[String]]]]] =
        Stream.eval(Ref[IO].of(0)).flatMap { counter =>
          Stream.unit.repeat.map { _ =>
            Stream
              .eval {
                (1 to config.eventsPerBatch).toList
                  .traverse(_ => counter.updateAndGet(_ + 1))
                  .map { numbers =>
                    val events  = numbers.map(_.toString)
                    val asBytes = Chunk.from(events).map(e => ByteBuffer.wrap(e.getBytes(StandardCharsets.UTF_8)))
                    Some(
                      LowLevelEvents(
                        events               = asBytes,
                        ack                  = events.toList,
                        earliestSourceTstamp = Some(config.streamTstamp.toEpochMilli.millis)
                      )
                    )
                  }
              }
              .flatMap { e =>
                Stream.emit(e) ++ Stream.sleep[IO](config.timeBetweenBatches).drain
              }
              .repeatN(config.batchesPerRebalance.toLong)
              .mergeHaltL(Stream.awakeDelay[IO](1.second).map(_ => None).repeat)
          }
        }
      def debounceCheckpoints: FiniteDuration = config.debounceCheckpoints
    }

}
