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
    fiberLocalPermitManager(sem).evalMap { permitManager =>
      // Must acquire a permit before attempting to access the resource.  Avoids conflicts with other concurrent fibers opening/closing the resource.
      (permitManager.acquireOnePermit *> ref.get).flatMap {
        case Opened(a, _) => Sync[F].pure(a)
        case Closed =>
          for {
            // Must acquire all permits before opening a new resource.  Avoids conflicts with other concurrent fibers that want to use the resource.
            _ <- permitManager.stepFromOneToAllPermits
            // We have all permits.  Safe to open the resource; this will not conflict with another fiber.
            a <- doOpen
            // Release all-but-one permits, to unblock other concurrent fibers from calling `coldswap.opened.use` in parallel.
            _ <- permitManager.stepFromAllToOnePermit
          } yield a
      }
    }

  /**
   * Closes the resource if it was open. The resource is guaranteed to remain closed for the
   * duration of the `Resource.use` block.
   */
  def closed: Resource[F, Unit] =
    fiberLocalPermitManager(sem).evalMap { permitManager =>
      // Must acquire a permit before inspecting the current state.  Avoids conflicts with other concurrent fibers opening/closing the resource.
      (permitManager.acquireOnePermit *> ref.get).flatMap {
        case Closed => Sync[F].unit
        case Opened(_, _) =>
          for {
            // Must acquire all permits before opening a new resource.  Avoids open/close conflicts with other concurrent fibers.
            _ <- permitManager.stepFromOneToAllPermits
            // We have all permits.  Safe to close the resource; this will not conflict with another fiber.
            _ <- doClose
            // Release all-but-one permits, to unblock other concurrent fibers from calling `coldswap.closed.use` in parallel.
            _ <- permitManager.stepFromAllToOnePermit
          } yield ()
      }
    }

  /**
   * Closes the currently held resource, if open.
   *
   * This must strictly only be called after acquiring ALL permits from the semaphore. Otherwise, we
   * would close the resource while someone is still using it.
   */
  private def doClose: F[Unit] =
    ref.get.flatMap {
      case Closed =>
        // This is possible if another fiber called `doClose` while we were waiting for this fiber to acquire all the permits
        Sync[F].unit
      case Opened(_, close) =>
        Sync[F].uncancelable { _ =>
          close *> ref.set(Closed)
        }
    }

  /**
   * Opens a new resource if currently closed.
   *
   * This must strictly only be called after acquiring ALL permits from the semaphore. Otherwise, we
   * would open the resource while someone still requires it to be closed.
   */
  private def doOpen: F[A] =
    ref.get.flatMap {
      case Opened(a, _) =>
        // This is possible if another fiber called `doOpen` while we were waiting for this fiber to acquire all the permits
        Sync[F].pure(a)
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

  private sealed trait PermitState

  /** State of a Fiber that has no permits */
  private case object HasNoPermits extends PermitState

  /**
   * State of a Fiber that has acquired one permit from the global Semaphore
   *
   * We must be in this State when Resource is being used by the calling app. It blocks another
   * fiber from opening/closing the Resource while it is still being used.
   */
  private case object HasOnePermit extends PermitState

  /**
   * State of a Fiber that has acquired all permits from the global Semaphore
   *
   * We must be in this State when opening/closing the Resource. It prevents another fiber from
   * accessing the Resource when it is not ready for use.
   */
  private case object HasAllPermits extends PermitState

  private val NumPermitsInExistence = Long.MaxValue

  /**
   * Pairs a Semaphore with a Ref which counts how many permits we have locally borrowed from the
   * Semaphore
   *
   * This is "local" in a sense that it belongs to a local fiber. It cannot be used for concurrent
   * access by multiple fibers.
   */
  private class FiberLocalPermitManager[F[_]: Sync](sem: Semaphore[F], state: Ref[F, PermitState]) {

    /**
     * Acquires one permit from the Semaphore and saves the state
     *
     * This MUST be called only when the local fiber currently has no permits.
     */
    def acquireOnePermit: F[Unit] =
      Sync[F].uncancelable { poll =>
        for {
          _ <- poll(sem.acquire)
          _ <- state.set(HasOnePermit)
        } yield ()
      }

    /**
     * Acquires all permits from the Semaphore and saves the state
     *
     * This MUST be called only when the local fiber currently has **one** permit.
     *
     * The implementation first releases the currently held permit before trying to acquire all
     * other permits. Otherwise two concurrent fibers might reach a deadlock both trying to step
     * from 1 to all permits.
     */
    def stepFromOneToAllPermits: F[Unit] =
      Sync[F].uncancelable { poll =>
        for {
          _ <- sem.release
          _ <- state.set(HasNoPermits)
          _ <- poll(sem.acquireN(NumPermitsInExistence))
          _ <- state.set(HasAllPermits)
        } yield ()
      }

    /**
     * Releases all-but-one permits from the Semaphore and saves the state
     *
     * This MUST be called only when the local fiber has **all** permits.
     */
    def stepFromAllToOnePermit: F[Unit] =
      Sync[F].uncancelable { _ =>
        for {
          _ <- sem.releaseN(NumPermitsInExistence - 1)
          _ <- state.set(HasOnePermit)
        } yield ()
      }
  }

  /**
   * Counts and cleans up locally held permits from a Semaphore
   *
   * The returned FiberLocalPermitManager must be used responsibly to acquire and release permits
   * from the semaphore. Any held permits will be released when this Resource is finalized.
   */
  private def fiberLocalPermitManager[F[_]: Sync](sem: Semaphore[F]): Resource[F, FiberLocalPermitManager[F]] =
    Resource.eval(Ref[F].of[PermitState](HasNoPermits)).flatMap { ref =>
      val finalizer = Resource.onFinalize {
        ref.get.flatMap {
          case HasNoPermits  => Sync[F].unit
          case HasOnePermit  => sem.release
          case HasAllPermits => sem.releaseN(NumPermitsInExistence)
        }
      }

      Resource.pure(new FiberLocalPermitManager(sem, ref)) <* finalizer
    }

}
