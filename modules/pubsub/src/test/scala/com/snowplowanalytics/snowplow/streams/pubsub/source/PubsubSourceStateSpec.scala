/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.IO
import cats.effect.kernel.Unique
import cats.effect.unsafe.implicits.global
import org.specs2.Specification

import java.time.Instant

import PubsubSourceState.{Pulling, Released}

class PubsubSourceStateSpec extends Specification {

  def is = s2"""
  The PubsubSourceState should:
    buffer a received batch while Pulling $e1
    hand over a buffered batch while Pulling, moving it to emitted $e2
    hand over an item without batches while Pulling, leaving the state as-is $e3
    move to Released, returning the buffered batches and keeping the emitted ones $e4
    hand nothing over once Released, including an item without batches $e5
    never release a batch that was already handed over $e6
    reject a batch received once Released $e7
    release nothing on a second call $e8
    remove batches for the checkpointer, buffered or emitted, in both states $e9
    remove every batch in both states, staying in the same state $e10
    extend near deadlines of buffered and emitted batches in both states $e11
  """

  private def tokens(n: Int): Vector[Unique.Token] =
    Vector.fill(n)(Unique[IO].unique).sequence.unsafeRunSync()

  private def batch(id: String, deadline: Instant = Instant.EPOCH): PubsubBatchState =
    PubsubBatchState(deadline, Vector(id))

  def e1 = {
    val Vector(t)     = tokens(1)
    val (s, accepted) = PubsubSourceState.initial.receive(t, batch("a"))
    (accepted must beTrue).toResult and (s must beEqualTo(Pulling(Map(t -> batch("a")), Map.empty))).toResult
  }

  def e2 = {
    val Vector(t1, t2) = tokens(2)
    val (s1, _)        = PubsubSourceState.initial.receive(t1, batch("a"))
    val (s2, _)        = s1.receive(t2, batch("b"))
    val (s3, handed)   = s2.handOver(Vector(t1))
    (handed must beTrue).toResult and
      (s3 must beEqualTo(Pulling(Map(t2 -> batch("b")), Map(t1 -> batch("a"))))).toResult
  }

  def e3 = {
    val Vector(t)    = tokens(1)
    val (s1, _)      = PubsubSourceState.initial.receive(t, batch("a"))
    val (s2, handed) = s1.handOver(Vector.empty)
    (handed must beTrue).toResult and (s2 must beEqualTo(s1)).toResult
  }

  def e4 = {
    val Vector(t1, t2, t3) = tokens(3)
    val (s1, _)            = PubsubSourceState.initial.receive(t1, batch("a"))
    val (s2, _)            = s1.receive(t2, batch("b"))
    val (s3, _)            = s2.receive(t3, batch("c"))
    val (s4, _)            = s3.handOver(Vector(t2))
    val (s5, released)     = s4.release
    (released.flatMap(_.ackIds).toSet must beEqualTo(Set("a", "c"))).toResult and
      (s5 must beEqualTo(Released(Map(t2 -> batch("b"))))).toResult and
      (s5.isReleased must beTrue).toResult
  }

  def e5 = {
    val Vector(t)        = tokens(1)
    val (s1, _)          = PubsubSourceState.initial.receive(t, batch("a"))
    val (s2, _)          = s1.release
    val (s3, handed1)    = s2.handOver(Vector(t))
    val (s4, handedNone) = s3.handOver(Vector.empty)
    (handed1 must beFalse).toResult and
      (handedNone must beFalse).toResult and
      (s4 must beEqualTo(Released(Map.empty))).toResult
  }

  def e6 = {
    val Vector(t)      = tokens(1)
    val (s1, _)        = PubsubSourceState.initial.receive(t, batch("a"))
    val (s2, _)        = s1.handOver(Vector(t))
    val (s3, released) = s2.release
    (released must beEmpty).toResult and (s3 must beEqualTo(Released(Map(t -> batch("a"))))).toResult
  }

  def e7 = {
    val Vector(t1, t2) = tokens(2)
    val (s1, _)        = PubsubSourceState.initial.receive(t1, batch("a"))
    val (s2, _)        = s1.release
    val (s3, accepted) = s2.receive(t2, batch("b"))
    (accepted must beFalse).toResult and (s3 must beEqualTo(s2)).toResult
  }

  def e8 = {
    val Vector(t)       = tokens(1)
    val (s1, _)         = PubsubSourceState.initial.receive(t, batch("a"))
    val (s2, released1) = s1.release
    val (s3, released2) = s2.release
    (released1.size must beEqualTo(1)).toResult and (released2 must beEmpty).toResult and (s3 must beEqualTo(s2)).toResult
  }

  def e9 = {
    val Vector(t1, t2, t3) = tokens(3)
    val (s1, _)            = PubsubSourceState.initial.receive(t1, batch("a"))
    val (s2, _)            = s1.receive(t2, batch("b"))
    val (s3, _)            = s2.handOver(Vector(t1))
    val (s4, removedP)     = s3.remove(Vector(t1, t2, t3))
    val (s5, _)            = s3.release
    val (s6, removedR)     = s5.remove(Vector(t1, t2))
    (removedP.flatMap(_.ackIds).toSet must beEqualTo(Set("a", "b"))).toResult and
      (s4 must beEqualTo(PubsubSourceState.initial)).toResult and
      (removedR.flatMap(_.ackIds) must beEqualTo(Vector("a"))).toResult and
      (s6 must beEqualTo(Released(Map.empty))).toResult
  }

  def e10 = {
    val Vector(t1, t2) = tokens(2)
    val (s1, _)        = PubsubSourceState.initial.receive(t1, batch("a"))
    val (s2, _)        = s1.receive(t2, batch("b"))
    val (s3, _)        = s2.handOver(Vector(t1))
    val (s4, allP)     = s3.removeAll
    val (s5, _)        = s3.release
    val (s6, allR)     = s5.removeAll
    (allP.flatMap(_.ackIds).toSet must beEqualTo(Set("a", "b"))).toResult and
      (s4 must beEqualTo(PubsubSourceState.initial)).toResult and
      (allR.flatMap(_.ackIds) must beEqualTo(Vector("a"))).toResult and
      (s6 must beEqualTo(Released(Map.empty))).toResult
  }

  def e11 = {
    val near               = Instant.ofEpochSecond(10)
    val far                = Instant.ofEpochSecond(1000)
    val minAllowed         = Instant.ofEpochSecond(100)
    val newDeadline        = Instant.ofEpochSecond(200)
    val Vector(t1, t2, t3) = tokens(3)
    val (s1, _)            = PubsubSourceState.initial.receive(t1, batch("a", near))
    val (s2, _)            = s1.receive(t2, batch("b", near))
    val (s3, _)            = s2.receive(t3, batch("c", far))
    val (s4, _)            = s3.handOver(Vector(t1))
    val (s5, extendedP)    = s4.extendDeadlines(minAllowed, newDeadline)
    val (s6, _)            = s4.release
    val (s7, extendedR)    = s6.extendDeadlines(minAllowed, newDeadline)
    (extendedP.flatMap(_.ackIds).toSet must beEqualTo(Set("a", "b"))).toResult and
      (s5 must beEqualTo(
        Pulling(Map(t2 -> batch("b", newDeadline), t3 -> batch("c", far)), Map(t1 -> batch("a", newDeadline)))
      )).toResult and
      (extendedR.flatMap(_.ackIds) must beEqualTo(Vector("a"))).toResult and
      (s7 must beEqualTo(Released(Map(t1 -> batch("a", newDeadline))))).toResult
  }
}
