/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import org.specs2.Specification

import scala.concurrent.duration._

class DecayingMaxSpec extends Specification with CatsEffect {

  def is = s2"""
  DecayingMax should:
    Return None when nothing has been recorded $empty
    Return a recorded value $single
    Return the largest of several values recorded in the same bucket $largest
    Still return a value just before the window has elapsed $beforeExpiry
    Return None once the window has fully elapsed $afterExpiry
    Expire an old value but keep a newer smaller one $partialExpiry
    Return the same answer when polled twice at the same instant $idempotent
    Keep a value for exactly the documented minimum when a record is not bucket-aligned $unalignedBeforeExpiry
    Not return a negative value $negative
  """

  private val window      = 60.seconds
  private val bucketCount = 3

  private def build: IO[DecayingMax[IO]] = DecayingMax.build[IO](window, bucketCount)

  def empty = TestControl.executeEmbed {
    for {
      dm <- build
      result <- dm.poll
    } yield result must beNone
  }

  def single = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(5.seconds)
      result <- dm.poll
    } yield result must beSome(5.seconds)
  }

  def largest = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(5.seconds)
      _ <- dm.record(9.seconds)
      _ <- dm.record(2.seconds)
      result <- dm.poll
    } yield result must beSome(9.seconds)
  }

  def beforeExpiry = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(5.seconds)
      _ <- IO.sleep(59.seconds)
      result <- dm.poll
    } yield result must beSome(5.seconds)
  }

  def afterExpiry = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(5.seconds)
      _ <- IO.sleep(60.seconds)
      result <- dm.poll
    } yield result must beNone
  }

  def partialExpiry = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(5.seconds)
      _ <- IO.sleep(30.seconds)
      _ <- dm.record(1.second)
      atThirty <- dm.poll
      _ <- IO.sleep(40.seconds)
      atSeventy <- dm.poll
    } yield List(
      atThirty must beSome(5.seconds),
      atSeventy must beSome(1.second)
    ).reduce(_ and _)
  }

  /**
   * `intervalOf` buckets the monotonic clock, which TestControl starts at zero, so every other
   * example happens to begin exactly on an interval boundary - the best case, where a value is
   * retained for the full `window`. That makes `beforeExpiry`'s poll at 59 seconds an alignment
   * artifact rather than a guarantee.
   *
   * This pins the documented worst case instead. With a 60-second window and 3 buckets the interval
   * is 20 seconds, so a value recorded at t=19.9s is still one of the three retained intervals at
   * t=59.9s and has rotated out at t=60.0s. Both halves matter: visible at exactly +40s and gone
   * 100ms later is the guarantee `window - window / bucketCount` = 40 seconds actually makes,
   * neither more nor less.
   */
  def unalignedBeforeExpiry = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- IO.sleep(19900.millis)
      _ <- dm.record(5.seconds)
      _ <- IO.sleep(40.seconds)
      atForty <- dm.poll
      _ <- IO.sleep(100.millis)
      atFortyPointOne <- dm.poll
    } yield List(
      atForty must beSome(5.seconds),
      atFortyPointOne must beNone
    ).reduce(_ and _)
  }

  /** See `DecayingMax.record` for why a negative is reachable at all. */
  def negative = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(-3.seconds)
      result <- dm.poll
    } yield result.map(_ >= Duration.Zero) must beSome(true)
  }

  def idempotent = TestControl.executeEmbed {
    for {
      dm <- build
      _ <- dm.record(3.seconds)
      _ <- IO.sleep(25.seconds)
      first <- dm.poll
      second <- dm.poll
    } yield List(
      first must beSome(3.seconds),
      second === first
    ).reduce(_ and _)
  }

}
