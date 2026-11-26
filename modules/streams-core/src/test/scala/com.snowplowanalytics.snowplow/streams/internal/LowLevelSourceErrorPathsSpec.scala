/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.effect.IO

import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect

import org.specs2.Specification
import fs2.CompositeFailure

import scala.concurrent.duration.DurationLong
import java.nio.charset.StandardCharsets

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, TokenedEvents}

/**
 * Part of `LowLevelSourceSpec`, in its own class because specs2's `s2` interpolator embeds the
 * spec's source text in a single string constant, and `LowLevelSourceSpec` is at the JVM's 64 KB
 * limit for one.
 */
class LowLevelSourceErrorPathsSpec extends Specification with CatsEffect {
  import LowLevelSourceSpec._
  import LowLevelSourceErrorPathsSpec._

  def is = s2"""
  A LowLevelSource raised to a SourceAndAck should:
    When an event processor stops consuming its window before the end:
      with no windowing, fail rather than hang $abandoned1
      with timed windows, fail rather than hang $abandoned2
    When the source raises an error after a window's event processor has failed:
      still record the source's error $sourceErrorAfterSinkGone
    When an eager window is abandoned because the previous window failed:
      fail with the cause, even when the abandonment arrived first $abandonedFirst
      keep the other errors, in the order they arrived $abandonedAmongOthers
      keep an abandonment that is the only error $abandonedAlone
  """

  def abandoned1 = {

    val config = EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 10.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = runFinalizingSource(config, testConfig, SourceFailures(), ref => in => testProcessor(ref, testConfig)(in.take(2)), 1.day)

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions must beEqualTo(
        Vector(
          Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2")),
          Action.Checkpointed(List("1", "2")),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:10Z", List("3", "4")),
          Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:11Z"),
          Action.Checkpointed(List("3", "4")),
          Action.Nacked(Nil),
          Action.SourceInnerFinalized(1),
          Action.SourceOuterFinalized
        )
      )) and (error must beSome(CleanCancellation.ProcessorFinishedEarly))
    }
  }

  def abandoned2 = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(60.seconds, 1.0, 2), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance = Int.MaxValue,
      eventsPerBatch      = 2,
      timeBetweenBatches  = 10.seconds,
      timeToProcessBatch  = 1.second
    )

    val io = runFinalizingSource(config, testConfig, SourceFailures(), ref => in => windowedProcessor(ref, testConfig)(in.take(2)), 1.day)

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions must beEqualTo(
        Vector(
          Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1", "2")),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:10Z", List("3", "4")),
          Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:11Z"),
          Action.Checkpointed(List("1", "2", "3", "4")),
          Action.Nacked(Nil),
          Action.SourceInnerFinalized(1),
          Action.SourceOuterFinalized
        )
      )) and (error must beSome(CleanCancellation.ProcessorFinishedEarly))
    }
  }

  def sourceErrorAfterSinkGone = {

    val config = EventProcessingConfig(EventProcessingConfig.TimedWindows(10.seconds, 1.0, 1), _ => IO.unit)

    val testConfig = TestSourceConfig(
      batchesPerRebalance  = Int.MaxValue,
      eventsPerBatch       = 1,
      timeBetweenBatches   = 4.seconds,
      timeToProcessBatch   = 1.second,
      timeToFinalizeWindow = 30.seconds
    )

    val failures = SourceFailures(innerErrorAfterBatches = Some(5))

    val io = runFinalizingSource(config, testConfig, failures, ref => failOnEvent("5")(windowedProcessor(ref, testConfig)), 1.day)

    TestControl.executeEmbed(io).map { case (actions, error) =>
      (actions.map(sortNacks) must beEqualTo(
        Vector(
          Action.ProcessorStartedWindow("1970-01-01T00:00:00Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:00Z", List("1")),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:04Z", List("2")),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:08Z", List("3")),
          Action.ProcessorReachedEndOfWindow("1970-01-01T00:00:10Z"),
          Action.ProcessorStartedWindow("1970-01-01T00:00:12Z"),
          Action.ProcessorReceivedEvents("1970-01-01T00:00:12Z", List("4")),
          Action.SourceInnerFinalized(1),
          Action.Checkpointed(List("1", "2", "3")),
          Action.Nacked(List("4", "5")),
          Action.SourceOuterFinalized
        )
      )) and (error must beSome(InnerSourceError))
    }
  }

  def abandonedFirst = {
    val cause = new RuntimeException("processor failed")
    val e     = CompositeFailure(new EagerWindows.AbandonedWindowException, cause)
    LowLevelSource.withoutAbandonedWindows(e) must beTheSameAs(cause)
  }

  def abandonedAmongOthers = {
    val first  = new RuntimeException("first")
    val second = new RuntimeException("second")
    val e      = CompositeFailure(new EagerWindows.AbandonedWindowException, first, List(new EagerWindows.AbandonedWindowException, second))
    LowLevelSource.withoutAbandonedWindows(e) must beLike { case cf: CompositeFailure =>
      cf.all.toList must beEqualTo(List(first, second))
    }
  }

  def abandonedAlone = {
    val e = new EagerWindows.AbandonedWindowException
    LowLevelSource.withoutAbandonedWindows(e) must beTheSameAs(e)
  }
}

object LowLevelSourceErrorPathsSpec {
  import LowLevelSourceSpec.Action

  /** A processor that raises an error when it sees the given event */
  def failOnEvent(event: String)(inner: EventProcessor[IO]): EventProcessor[IO] = { in =>
    inner(in.evalTap { case TokenedEvents(events, _) =>
      val deserialized = events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer.duplicate).toString).toList
      if (deserialized.contains(event)) IO.raiseError(new RuntimeException(FailedOnEvent)) else IO.unit
    })
  }

  val FailedOnEvent = "processor failed on an event"

  // The order of nacked events follows a `Map` keyed by `Unique.Token`, so it is not deterministic
  def sortNacks(action: Action): Action =
    action match {
      case Action.Nacked(events) => Action.Nacked(events.sortBy(_.toInt))
      case other                 => other
    }

}
