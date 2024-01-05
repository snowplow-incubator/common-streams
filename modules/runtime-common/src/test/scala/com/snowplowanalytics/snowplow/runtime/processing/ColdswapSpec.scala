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
    Block closing a release until it is released by concurrent fiber using the resource $e5
    Re-open a resource after closing it $e6
    Respect the order of conflicting requests to open/close a resource $e7
    Manage resource lifecycle when the inner IO is cancelled $e8
    Manage resource lifecycle when the inner IO raises an exception $e9
    Recover after an inner IO raises an exception $e10
  The Coldswap with a resource that raises an exception on first open:
    Take no action if the resource is never successfully opened $u1
    Recover and manage resource lifecycle after second attempt to open $u2
  """

  def e1 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      val _ = coldswap // don't use it
      IO.unit
    }

    for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
      )
    )
  }

  def e2 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      useOpenedResource(coldswap, ref)
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        0.seconds -> Action.UsedWhenOpened,
        0.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e3 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        _ <- useOpenedResource(coldswap, ref)
        _ <- IO.sleep(2.seconds)
        _ <- useOpenedResource(coldswap, ref)
        _ <- IO.sleep(2.seconds)
        _ <- useOpenedResource(coldswap, ref)
      } yield ()
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        0.seconds -> Action.UsedWhenOpened,
        2.seconds -> Action.UsedWhenOpened,
        4.seconds -> Action.UsedWhenOpened,
        4.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e4 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
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

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        10.seconds -> Action.UsedWhenOpened,
        12.seconds -> Action.UsedWhenOpened,
        14.seconds -> Action.UsedWhenOpened,
        14.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e5 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        fiber <- useOpenedResource(coldswap, ref, withDelay = 100.seconds).start
        _ <- IO.sleep(1.seconds)
        _ <- useClosedResource(coldswap, ref)
        _ <- fiber.join
      } yield ()
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        100.seconds -> Action.UsedWhenOpened,
        100.seconds -> Action.ClosedResource,
        100.seconds -> Action.UsedWhenClosed
      )
    )
    TestControl.executeEmbed(test)
  }

  def e6 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        _ <- useOpenedResource(coldswap, ref, withDelay = 1.seconds)
        _ <- IO.sleep(1.seconds)
        _ <- useClosedResource(coldswap, ref, withDelay = 1.seconds)
        _ <- IO.sleep(1.seconds)
        _ <- useOpenedResource(coldswap, ref, withDelay = 1.seconds)
      } yield ()
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        1.seconds -> Action.UsedWhenOpened,
        2.seconds -> Action.ClosedResource,
        3.seconds -> Action.UsedWhenClosed,
        4.seconds -> Action.OpenedResource,
        5.seconds -> Action.UsedWhenOpened,
        5.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e7 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        fiber1 <- useOpenedResource(coldswap, ref, withDelay = 100.seconds).start
        _ <- IO.sleep(1.seconds)
        fiber2 <- useClosedResource(coldswap, ref, withDelay = 1.seconds).start
        _ <- IO.sleep(1.seconds)
        fiber3 <- useOpenedResource(coldswap, ref, withDelay = 1.seconds).start
        _ <- fiber1.join
        _ <- fiber2.join
        _ <- fiber3.join
      } yield ()
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        100.seconds -> Action.UsedWhenOpened,
        100.seconds -> Action.ClosedResource,
        101.seconds -> Action.UsedWhenClosed,
        101.seconds -> Action.OpenedResource,
        102.seconds -> Action.UsedWhenOpened,
        102.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e8 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        fiber <- coldswap.opened.use(_ => IO.never).start
        _ <- IO.sleep(1.seconds)
        _ <- fiber.cancel
      } yield ()
    }

    val test = for {
      _ <- io
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        1.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e9 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      coldswap.opened.use(_ => IO.raiseError(new RuntimeException("boom!")))
    }

    val test = for {
      _ <- io.voidError
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        0.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def e10 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = Coldswap.make(healthyResource(ref)).use { coldswap =>
      for {
        _ <- coldswap.opened.use(_ => IO.raiseError(new RuntimeException("boom!"))).voidError
        _ <- useOpenedResource(coldswap, ref)
      } yield ()
    }

    val test = for {
      _ <- io.voidError
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        0.seconds -> Action.UsedWhenOpened,
        0.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

  def u1 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = errorOnFirstOpenResource(ref).flatMap { r =>
      Coldswap.make(r).use { coldswap =>
        useOpenedResource(coldswap, ref)
      }
    }

    val test = for {
      _ <- io.voidError
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
      )
    )
    TestControl.executeEmbed(test)
  }

  def u2 = Ref[IO].of(Vector.empty[(FiniteDuration, Action)]).flatMap { ref =>
    val io = errorOnFirstOpenResource(ref).flatMap { r =>
      Coldswap.make(r).use { coldswap =>
        for {
          _ <- useOpenedResource(coldswap, ref).voidError
          _ <- useOpenedResource(coldswap, ref)
        } yield ()
      }
    }

    val test = for {
      _ <- io.voidError
      state <- ref.get
    } yield state must beEqualTo(
      Vector(
        0.seconds -> Action.OpenedResource,
        0.seconds -> Action.UsedWhenOpened,
        0.seconds -> Action.ClosedResource
      )
    )
    TestControl.executeEmbed(test)
  }

}

object ColdswapSpec {

  sealed trait Action
  object Action {
    case object OpenedResource extends Action
    case object ClosedResource extends Action
    case object UsedWhenOpened extends Action
    case object UsedWhenClosed extends Action
  }

  def healthyResource(state: Ref[IO, Vector[(FiniteDuration, Action)]]): Resource[IO, Unit] = {
    val make = for {
      now <- IO.realTime
      _ <- state.update(_ :+ now -> Action.OpenedResource)
    } yield ()

    val close = for {
      now <- IO.realTime
      _ <- state.update(_ :+ now -> Action.ClosedResource)
    } yield ()

    Resource.make(make)(_ => close)
  }

  def errorOnFirstOpenResource(state: Ref[IO, Vector[(FiniteDuration, Action)]]): IO[Resource[IO, Unit]] =
    Ref[IO].of(false).map { ref =>
      Resource.eval {
        for {
          hasThrownAlready <- ref.getAndSet(true)
          _ <- if (hasThrownAlready) IO.unit else IO.raiseError(new RuntimeException("boom!"))
        } yield ()
      } *> healthyResource(state)
    }

  def useOpenedResource(
    coldswap: Coldswap[IO, Unit],
    state: Ref[IO, Vector[(FiniteDuration, Action)]],
    withDelay: FiniteDuration = 0.seconds
  ): IO[Unit] =
    coldswap.opened.use { _ =>
      for {
        _ <- IO.sleep(withDelay)
        now <- IO.realTime
        _ <- state.update(_ :+ now -> Action.UsedWhenOpened)
      } yield ()
    }

  def useClosedResource(
    coldswap: Coldswap[IO, Unit],
    state: Ref[IO, Vector[(FiniteDuration, Action)]],
    withDelay: FiniteDuration = 0.seconds
  ): IO[Unit] =
    coldswap.closed.use { _ =>
      for {
        _ <- IO.sleep(withDelay)
        now <- IO.realTime
        _ <- state.update(_ :+ now -> Action.UsedWhenClosed)
      } yield ()
    }

}
