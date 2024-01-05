/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Semaphore
import cats.implicits._

/**
 * Manages swapping of Resources
 *
 * Inspired by `cats.effect.std.Hotswap` but with differences. A Hotswap is "hot" because a `swap`
 * acquires the next resource before closing the previous one. Whereas this Coldswap is "cold"
 * because it always closes any previous Resources before acquiring the next one.
 *
 * * '''Note''': The resource cannot be simultaneously open and closed, and so
 * `coldswap.opened.surround(coldswap.closed.use_)` will deadlock.
 */
final class Coldswap[F[_]: Sync, A] private (
  sem: Semaphore[F],
  ref: Ref[F, Coldswap.State[F, A]],
  resource: Resource[F, A]
) {
  import Coldswap._

  /**
   * Gets the current resource, or opens a new one if required. The returned `A` is guaranteed to be
   * available for the duration of the `Resource.use` block.
   */
  def opened: Resource[F, A] =
    trackHeldPermits(sem).evalMap { permitManager =>
      (permitManager.acquireN(1L) *> ref.get).flatMap {
        case Opened(a, _) => Sync[F].pure(a)
        case Closed =>
          for {
            _ <- permitManager.releaseN(1L)
            _ <- permitManager.acquireN(Long.MaxValue)
            a <- doOpen(ref, resource)
            _ <- permitManager.releaseN(Long.MaxValue - 1)
          } yield a
      }
    }

  /**
   * Closes the resource if it was open. The resource is guaranteed to remain closed for the
   * duration of the `Resource.use` block.
   */
  def closed: Resource[F, Unit] =
    trackHeldPermits(sem).evalMap { permitManager =>
      (permitManager.acquireN(1L) *> ref.get).flatMap {
        case Closed => Sync[F].unit
        case Opened(_, _) =>
          for {
            _ <- permitManager.releaseN(1L)
            _ <- permitManager.acquireN(Long.MaxValue)
            _ <- doClose(ref)
            _ <- permitManager.releaseN(Long.MaxValue - 1)
          } yield ()
      }
    }

  private def doClose(ref: Ref[F, State[F, A]]): F[Unit] =
    ref.get.flatMap {
      case Closed => Sync[F].unit
      case Opened(_, close) =>
        Sync[F].uncancelable { _ =>
          close *> ref.set(Closed)
        }
    }

  private def doOpen(ref: Ref[F, State[F, A]], resource: Resource[F, A]): F[A] =
    ref.get.flatMap {
      case Opened(a, _) => Sync[F].pure(a)
      case Closed =>
        Sync[F].uncancelable { poll =>
          for {
            (a, close) <- poll(resource.allocated)
            _ <- ref.set(Opened(a, close))
          } yield a
        }
    }

}

object Coldswap {

  private sealed trait State[+F[_], +A]
  private case object Closed extends State[Nothing, Nothing]
  private case class Opened[F[_], A](value: A, close: F[Unit]) extends State[F, A]

  def make[F[_]: Async, A](resource: Resource[F, A]): Resource[F, Coldswap[F, A]] =
    for {
      sem <- Resource.eval(Semaphore[F](Long.MaxValue))
      ref <- Resource.eval(Ref.of[F, State[F, A]](Closed))
      coldswap = new Coldswap(sem, ref, resource)
      _ <- Resource.onFinalize(coldswap.closed.use_)
    } yield coldswap

  /**
   * Pairs a Semaphore with a Ref which counts how many permits we have locally borrowed from the
   * Semaphore
   */
  private class PermitManager[F[_]: Sync](sem: Semaphore[F], held: Ref[F, Long]) {
    def acquireN(count: Long): F[Unit] =
      Sync[F].uncancelable { poll =>
        for {
          _ <- poll(sem.acquireN(count))
          _ <- held.update(_ + count)
        } yield ()
      }

    def releaseN(count: Long): F[Unit] =
      Sync[F].uncancelable { _ =>
        for {
          _ <- sem.releaseN(count)
          _ <- held.update(_ - count)
        } yield ()
      }
  }

  /**
   * Counts and cleans up locally held permits from a Semaphore
   *
   * The returned PermitManager must be used responsibly to acquire and release permits from the
   * semaphore. Any held permits will be released when this Resource is finalized.
   */
  private def trackHeldPermits[F[_]: Sync](sem: Semaphore[F]): Resource[F, PermitManager[F]] =
    Resource.eval(Ref[F].of(0L)).flatMap { ref =>
      val finalizer = Resource.onFinalize {
        for {
          count <- ref.get
          _ <- sem.releaseN(count)
        } yield ()
      }

      Resource.pure(new PermitManager(sem, ref)) <* finalizer
    }

}
