/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.streams.EventProcessingConfig

/**
 * Part of `LowLevelSourceSpec`, in its own class because specs2's `s2` interpolator embeds the
 * spec's source text in a single string constant, and `LowLevelSourceSpec` is at the JVM's 64 KB
 * limit for one.
 */
class LowLevelSourceNoEagerWindowsSpec extends Specification with CatsEffect {
  import LowLevelSourceSpec._

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck should:
    With a processor that operates on windows of events:
      keep opening and checkpointing windows when no window is started eagerly $windowed7
  """

  def windowed7 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(10.seconds, 1.0, numEagerWindows = 0), _ => IO.unit)

    // Batches every 3 seconds never arrive at the instant a window times out
    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 1,
      timeBetweenBatches  = 3.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = for {
      refActions <- Ref[IO].of(Vector.empty[Action])
      sourceAndAck <- LowLevelSource.toSourceAndAck(testLowLevelSource(refActions, testConfig))
      processor = windowedProcessor(refActions, testConfig)
      fiber <- sourceAndAck.stream(config, processor).compile.drain.start
      _ <- IO.sleep(59.seconds)
      _ <- fiber.cancel
      result <- refActions.get
    } yield result must beEqualTo(
      Vector(
        Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:03Z", List("2")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:06Z", List("3")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:09Z", List("4")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:10Z"),
        Action.Checkpointed(List("1", "2", "3", "4")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:12Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:12Z", List("5")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:15Z", List("6")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:18Z", List("7")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:21Z", List("8")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:22Z"),
        Action.Checkpointed(List("5", "6", "7", "8")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:24Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:24Z", List("9")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:27Z", List("10")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:30Z", List("11")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:33Z", List("12")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:34Z"),
        Action.Checkpointed(List("9", "10", "11", "12")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:36Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:36Z", List("13")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:39Z", List("14")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:42Z", List("15")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:45Z", List("16")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:46Z"),
        Action.Checkpointed(List("13", "14", "15", "16")),
        Action.ProcessorStartedWindow("1970-01-01T00:00:48Z"),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:48Z", List("17")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:51Z", List("18")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:54Z", List("19")),
        Action.ProcessorReceivedEvents("1970-01-01T00:00:57Z", List("20")),
        Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:58Z"),
        Action.Checkpointed(List("17", "18", "19", "20"))
      )
    )

    TestControl.executeEmbed(io)
  }
}
