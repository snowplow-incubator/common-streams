/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import fs2.Stream
import org.specs2.Specification

import scala.concurrent.duration.DurationLong

class BatchUpSpec extends Specification with CatsEffect {
  import BatchUpSpec._

  def is = s2"""
  The BatchUp pipe without timeout should:
    Combine strings, respecting max size $noTimeout1
    Combine strings, respecting max size for elements of different sizes $noTimeout2
    Combine strings, respecting max size, for Stream comprising random-sized Chunks $noTimeout3
    Wait until stream ends before emitting an under-sized element $noTimeout4
  The BatchUp pipe with timeout should:
    Combine strings, respecting max size $timeout1
    Combine strings, respecting max size, for Stream comprising random-sized Chunks $timeout2
    Emit an undersized element early, respecting the max allowed delay $timeout3
    Wait for elements within the allowed delay $timeout4
    Wait until stream ends before emitting an under-sized element $timeout5
  """

  def noTimeout1 = {
    val input    = Stream("a", "b", "c", "d", "e", "f", "g", "h")
    val pipe     = BatchUp.noTimeout[IO, String, String](3)
    val expected = List("abc", "def", "gh")

    for {
      result <- input.through(pipe).compile.toList
    } yield result must beEqualTo(expected)
  }

  def noTimeout2 = {
    val input    = Stream("a", "bb", "ccc", "dd", "eeeee", "f", "gggggggggg")
    val pipe     = BatchUp.noTimeout[IO, String, String](5)
    val expected = List("abb", "cccdd", "eeeee", "f", "gggggggggg")

    for {
      result <- input.through(pipe).compile.toList
    } yield result must beEqualTo(expected)
  }

  def noTimeout3 = {
    val input    = Stream("a", "b", "c", "d", "e", "f", "g", "h").rechunkRandomly(0.0, 2.0)
    val pipe     = BatchUp.noTimeout[IO, String, String](3)
    val expected = List("abc", "def", "gh")

    for {
      result <- input.through(pipe).compile.toList
    } yield result must beEqualTo(expected)
  }

  def noTimeout4 = {
    val input = Stream("a", "b", "c", "d") ++ Stream.sleep_[IO](5.minutes)
    val pipe  = BatchUp.noTimeout[IO, String, String](3)

    val test = input.through(pipe).evalMap { str =>
      // emit the string + the time the string was emitted by the pipe under test
      IO.realTime.map(now => str -> now)
    }

    val expected = List("abc" -> 0.seconds, "d" -> 5.minutes)

    val io = for {
      result <- test.compile.toList
    } yield result must beEqualTo(expected)

    TestControl.executeEmbed(io)
  }

  def timeout1 = {
    val input    = Stream("a", "b", "c", "d", "e", "f", "g", "h")
    val pipe     = BatchUp.withTimeout[IO, String, String](3, 1.second)
    val expected = List("abc", "def", "gh")

    for {
      result <- input.through(pipe).compile.toList
    } yield result must beEqualTo(expected)
  }

  def timeout2 = {
    val input    = Stream("a", "b", "c", "d", "e", "f", "g", "h").rechunkRandomly(0.0, 2.0)
    val pipe     = BatchUp.withTimeout[IO, String, String](3, 1.second)
    val expected = List("abc", "def", "gh")

    for {
      result <- input.through(pipe).compile.toList
    } yield result must beEqualTo(expected)
  }

  def timeout3 = {
    val input = Stream("a", "b") ++ Stream.sleep_[IO](5.minutes) ++ Stream("c", "d", "e", "f")
    val pipe  = BatchUp.withTimeout[IO, String, String](3, 1.second)

    val test = input.through(pipe).evalMap { str =>
      // emit the string + the time the string was emitted by the pipe under test
      IO.realTime.map(now => str -> now)
    }

    val expected = List("ab" -> 1.second, "cde" -> 5.minutes, "f" -> 5.minutes)

    val io = for {
      result <- test.compile.toList
    } yield result must beEqualTo(expected)

    TestControl.executeEmbed(io)
  }

  def timeout4 = {
    val input = Stream("a", "b") ++ Stream.sleep_[IO](5.seconds) ++ Stream("c", "d")
    val pipe  = BatchUp.withTimeout[IO, String, String](3, 10.seconds)

    val test = input.through(pipe).evalMap { str =>
      // emit the string + the time the string was emitted by the pipe under test
      IO.realTime.map(now => str -> now)
    }

    val expected = List("abc" -> 5.seconds, "d" -> 5.seconds)

    val io = for {
      result <- test.compile.toList
    } yield result must beEqualTo(expected)

    TestControl.executeEmbed(io)
  }

  def timeout5 = {
    val input = Stream("a", "b", "c", "d") ++ Stream.sleep_[IO](10.seconds)
    val pipe  = BatchUp.withTimeout[IO, String, String](3, 60.seconds)

    val test = input.through(pipe).evalMap { str =>
      // emit the string + the time the string was emitted by the pipe under test
      IO.realTime.map(now => str -> now)
    }

    val expected = List("abc" -> 0.seconds, "d" -> 10.seconds)

    val io = for {
      result <- test.compile.toList
    } yield result must beEqualTo(expected)

    TestControl.executeEmbed(io)
  }

}

object BatchUpSpec {

  implicit val stringBatchable: BatchUp.Batchable[String, String] = new BatchUp.Batchable[String, String] {
    def single(a: String): String             = a
    def combine(b: String, a: String): String = s"$b$a"
    def weightOf(a: String): Long             = a.length.toLong
  }

}
