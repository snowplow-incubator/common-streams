/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.implicits._
import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import org.specs2.Specification

import scala.concurrent.duration.{DurationLong, FiniteDuration}

class ColdswapSpec extends Specification with CatsEffect {
  import ColdswapSpec._

  def is = s2"""
  The Coldswap with a healthy resource:
    Take no action if the coldswap is never used $e1
    Manage resource lifecycle if the coldswap is opened $e2
    Re-use an opened resource when it is used sequentially $e3
    Re-use an opened resource when it is used by multiple fibers concurrently $e4
    Manage resource lifecycle when the inner IO is cancelled $e5
    Manage resource lifecycle when the inner IO raises an exception $e6
    Recover after an inner IO raises an exception $e7
  The Coldswap when a resource is invalidated:
    Replace the resource on the next use after an invalidation $i1
    Close and open exactly once after several concurrent invalidations of one resource $i2
    Ignore an invalidation through a stale lease $i3
    Not hand the broken resource to new fibers, and close it only after every holder released it $i4
    Swallow an error closing the broken resource and still open the replacement $i5
    Close the current resource on shutdown after a replacement $i6
    Close a broken resource on shutdown $i7
  The Coldswap with a resource that raises an exception on first open:
    Take no action if the resource is never successfully opened $u1
    Recover and manage resource lifecycle after second attempt to open $u2
  """

  def e1 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        val _ = coldswap // don't use it
        IO.unit
      }
      .as(ref)
  }.map(_ must beEqualTo(Vector()))

  def e2 = run { (ref, r) =>
    Coldswap.make(r).use(useOpenedResource(_, ref)).as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Used(1),
        0.seconds -> Closed(1)
      )
    )
  )

  def e3 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          _ <- useOpenedResource(coldswap, ref)
          _ <- IO.sleep(2.seconds)
          _ <- useOpenedResource(coldswap, ref)
          _ <- IO.sleep(2.seconds)
          _ <- useOpenedResource(coldswap, ref)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Used(1),
        2.seconds -> Used(1),
        4.seconds -> Used(1),
        4.seconds -> Closed(1)
      )
    )
  )

  def e4 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          fiber1 <- useOpenedResource(coldswap, ref, withDelay = 10.seconds).start
          _ <- IO.sleep(2.seconds)
          fiber2 <- useOpenedResource(coldswap, ref, withDelay = 10.seconds).start
          _ <- IO.sleep(2.seconds)
          fiber3 <- useOpenedResource(coldswap, ref, withDelay = 10.seconds).start
          _ <- fiber1.join
          _ <- fiber2.join
          _ <- fiber3.join
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        10.seconds -> Used(1),
        12.seconds -> Used(1),
        14.seconds -> Used(1),
        14.seconds -> Closed(1)
      )
    )
  )

  def e5 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          fiber <- coldswap.opened.use(_ => IO.never).start
          _ <- IO.sleep(1.seconds)
          _ <- fiber.cancel
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        1.seconds -> Closed(1)
      )
    )
  )

  def e6 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        coldswap.opened.use(_ => IO.raiseError[Unit](new RuntimeException("boom!")))
      }
      .voidError
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Closed(1)
      )
    )
  )

  def e7 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          _ <- coldswap.opened.use(_ => IO.raiseError[Unit](new RuntimeException("boom!"))).voidError
          _ <- useOpenedResource(coldswap, ref)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Used(1),
        0.seconds -> Closed(1)
      )
    )
  )

  def i1 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          _ <- coldswap.opened.use(_.invalidate)
          _ <- IO.sleep(1.second)
          _ <- useOpenedResource(coldswap, ref)
          _ <- IO.sleep(1.second)
          _ <- useOpenedResource(coldswap, ref)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        1.seconds -> Closed(1),
        1.seconds -> Opened(2),
        1.seconds -> Used(2),
        2.seconds -> Used(2),
        2.seconds -> Closed(2)
      )
    )
  )

  def i2 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        // Each of three concurrent holders of resource 1 invalidates it twice
        val invalidateTwice = coldswap.opened.use { lease =>
          IO.sleep(1.second) *> lease.invalidate *> IO.sleep(9.seconds) *> lease.invalidate
        }
        for {
          fibers <- List.fill(3)(invalidateTwice.start).sequence
          _ <- fibers.traverse_(_.join)
          others <- List.fill(3)(useOpenedResource(coldswap, ref).start).sequence
          _ <- others.traverse_(_.join)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        10.seconds -> Closed(1),
        10.seconds -> Opened(2),
        10.seconds -> Used(2),
        10.seconds -> Used(2),
        10.seconds -> Used(2),
        10.seconds -> Closed(2)
      )
    )
  )

  def i3 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          staleLease <- coldswap.opened.use(lease => lease.invalidate.as(lease))
          _ <- IO.sleep(1.second)
          _ <- useOpenedResource(coldswap, ref)
          _ <- staleLease.invalidate
          _ <- IO.sleep(1.second)
          _ <- useOpenedResource(coldswap, ref)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        1.seconds -> Closed(1),
        1.seconds -> Opened(2),
        1.seconds -> Used(2),
        2.seconds -> Used(2),
        2.seconds -> Closed(2)
      )
    )
  )

  def i4 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          // Two holders of resource 1. The first invalidates it at 1s and releases at 50s; the second releases at 100s.
          holder1 <- coldswap.opened.use { lease =>
                       IO.sleep(1.second) *> lease.invalidate *> IO.sleep(49.seconds) *> recordUse(lease, ref)
                     }.start
          holder2 <- useOpenedResource(coldswap, ref, withDelay = 100.seconds).start
          _ <- IO.sleep(2.seconds)
          // Requested after the invalidation, while resource 1 is still held
          late1 <- useOpenedResource(coldswap, ref).start
          _ <- IO.sleep(1.seconds)
          late2 <- useOpenedResource(coldswap, ref).start
          _ <- List(holder1, holder2, late1, late2).traverse_(_.join)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        50.seconds -> Used(1),
        100.seconds -> Used(1),
        100.seconds -> Closed(1),
        100.seconds -> Opened(2),
        100.seconds -> Used(2),
        100.seconds -> Used(2),
        100.seconds -> Closed(2)
      )
    )
  )

  def i5 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val test = for {
      r <- resource(ref, failCloseOf = Set(1))
      _ <- Coldswap.make(r).use { coldswap =>
             for {
               _ <- coldswap.opened.use(_.invalidate)
               _ <- useOpenedResource(coldswap, ref)
             } yield ()
           }
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Closed(1),
        0.seconds -> Opened(2),
        0.seconds -> Used(2),
        0.seconds -> Closed(2)
      )
    )
    TestControl.executeEmbed(test)
  }

  def i6 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        for {
          _ <- coldswap.opened.use(_.invalidate)
          _ <- useOpenedResource(coldswap, ref)
          _ <- IO.sleep(5.seconds)
        } yield ()
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Closed(1),
        0.seconds -> Opened(2),
        0.seconds -> Used(2),
        5.seconds -> Closed(2)
      )
    )
  )

  def i7 = run { (ref, r) =>
    Coldswap
      .make(r)
      .use { coldswap =>
        coldswap.opened.use(_.invalidate) *> IO.sleep(5.seconds)
      }
      .as(ref)
  }.map(
    _ must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        5.seconds -> Closed(1)
      )
    )
  )

  def u1 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val test = for {
      r <- errorOnFirstOpenResource(ref)
      _ <- Coldswap.make(r).use(useOpenedResource(_, ref)).voidError
      state <- ref.get
    } yield state must beEqualTo(Vector())
    TestControl.executeEmbed(test)
  }

  def u2 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val test = for {
      r <- errorOnFirstOpenResource(ref)
      _ <- Coldswap.make(r).use { coldswap =>
             for {
               _ <- useOpenedResource(coldswap, ref).voidError
               _ <- useOpenedResource(coldswap, ref)
             } yield ()
           }
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Opened(1),
        0.seconds -> Used(1),
        0.seconds -> Closed(1)
      )
    )
    TestControl.executeEmbed(test)
  }

}

object ColdswapSpec {

  /** Events recorded by the test resource. The Int identifies which opening of the resource. */
  sealed trait Action
  case class Opened(n: Int) extends Action
  case class Closed(n: Int) extends Action
  case class Used(n: Int) extends Action

  type Log = Ref[IO, Vector[(FiniteDuration, Action)]]

  /**
   * Runs a test against a healthy resource under TestControl, and returns what the resource
   * recorded
   */
  def run(f: (Log, Resource[IO, Int]) => IO[Log]): IO[Vector[(FiniteDuration, Action)]] =
    TestControl.executeEmbed {
      for {
        ref <- Ref[IO].of(Vector.empty[(FiniteDuration, Action)])
        r <- resource(ref)
        _ <- f(ref, r)
        state <- ref.get
      } yield state
    }

  /** A resource whose value is a counter of how many times it was opened */
  def resource(state: Log, failCloseOf: Set[Int] = Set.empty): IO[Resource[IO, Int]] =
    Ref[IO].of(0).map { counter =>
      val make = for {
        n <- counter.updateAndGet(_ + 1)
        now <- IO.realTime
        _ <- state.update(_ :+ now -> Opened(n))
      } yield n

      def close(n: Int) = for {
        now <- IO.realTime
        _ <- state.update(_ :+ now -> Closed(n))
        _ <- if (failCloseOf.contains(n)) IO.raiseError(new RuntimeException("boom on close!")) else IO.unit
      } yield ()

      Resource.make(make)(close)
    }

  def errorOnFirstOpenResource(state: Log): IO[Resource[IO, Int]] =
    for {
      thrown <- Ref[IO].of(false)
      r <- resource(state)
    } yield Resource.eval {
      thrown.getAndSet(true).flatMap { hasThrownAlready =>
        if (hasThrownAlready) IO.unit else IO.raiseError(new RuntimeException("boom!"))
      }
    } *> r

  def recordUse(lease: Coldswap.Lease[IO, Int], state: Log): IO[Unit] =
    for {
      now <- IO.realTime
      _ <- state.update(_ :+ now -> Used(lease.value))
    } yield ()

  def useOpenedResource(
    coldswap: Coldswap[IO, Int],
    state: Log,
    withDelay: FiniteDuration = 0.seconds
  ): IO[Unit] =
    coldswap.opened.use { lease =>
      IO.sleep(withDelay) *> recordUse(lease, state)
    }

}
