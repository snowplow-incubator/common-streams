/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.effect.{Async, Ref, Resource, Sync, Unique}
import cats.effect.std.Semaphore
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * Manages a lazily-opened Resource that is shared by concurrent fibers and replaced when a user
 * finds it broken.
 *
 * Unlike `cats.effect.std.Hotswap`, this Coldswap is "cold": it always closes the previous resource
 * before acquiring the next one.
 *
 * The contract:
 *   - Nothing is opened until the first `opened`. The resource then stays open and is shared by
 *     every concurrent `opened` until it is invalidated, or until the Coldswap is finalized.
 *   - `Lease.invalidate` marks the leased resource as broken without closing it. Fibers already
 *     holding it carry on; new `opened` requests queue. When the last holder releases, the first
 *     queued fiber closes the broken resource and opens a replacement.
 *   - Never call `opened`, directly or through anything the block waits on, from inside an
 *     `opened.use` block. If any fiber invalidates the resource in the meantime, the nested
 *     `opened` waits for every holder to release, including the outer lease, and deadlocks. To
 *     retry after invalidating, release the lease and call `opened` again.
 *   - A lease only ever invalidates the resource it was handed. Invalidating through a lease whose
 *     resource was already replaced is a no-op, so any number of invalidations of one resource
 *     cause exactly one close and one open.
 *   - An error from closing a broken resource is logged and swallowed, and the replacement still
 *     opens. An error from closing a healthy resource on finalization is raised.
 */
final class Coldswap[F[_]: Sync, A] private (
  sem: Semaphore[F],
  ref: Ref[F, Coldswap.State[F, A]],
  resource: Resource[F, A]
) {
  import Coldswap._

  /**
   * Leases the current resource, opening a replacement first if there is none or it is broken. The
   * leased resource stays open for the duration of the `Resource.use` block.
   */
  def opened: Resource[F, Lease[F, A]] =
    fiberLocalPermitManager(sem).evalMap { permitManager =>
      // Must acquire a permit before accessing the resource. Holding a permit blocks other fibers from closing it.
      (permitManager.acquireOnePermit *> ref.get).flatMap {
        case o @ Opened(_, _, _, false) => Sync[F].pure(leaseOf(o))
        case _ =>
          for {
            // Holding all permits means no other fiber is using the resource. Semaphore is FIFO, so
            // waiting here also blocks every later `opened` from being handed the broken resource.
            _ <- permitManager.stepFromOneToAllPermits
            o <- doReplace
            // Unblock other fibers waiting to use the resource.
            _ <- permitManager.stepFromAllToOnePermit
          } yield leaseOf(o)
      }
    }

  private def leaseOf(o: Opened[F, A]): Lease[F, A] =
    new Lease[F, A] {
      override val value: A = o.value
      override val invalidate: F[Unit] =
        ref.update {
          case current @ Opened(_, _, token, _) if token == o.token => current.copy(broken = true)
          case other                                                => other
        }
    }

  /** Closes the resource if open. Used by the finalizer of `Coldswap.make`. */
  private def shutdown: F[Unit] =
    fiberLocalPermitManager(sem).use { permitManager =>
      permitManager.acquireOnePermit *> permitManager.stepFromOneToAllPermits *> doClose
    }

  /**
   * Closes the currently held resource, if open. Errors are swallowed only if the resource is
   * broken.
   *
   * This must strictly only be called after acquiring ALL permits from the semaphore. Otherwise, we
   * would close the resource while someone is still using it.
   */
  private def doClose: F[Unit] =
    ref.get.flatMap {
      case Closed => Sync[F].unit
      case o @ Opened(_, _, _, _) =>
        Sync[F].uncancelable { _ =>
          val close =
            if (o.broken)
              o.close.handleErrorWith { e =>
                Logger[F].warn(e)("Error closing a resource that was already invalidated. Ignoring.")
              }
            else o.close
          close *> ref.set(Closed)
        }
    }

  /**
   * Returns the current resource if it is healthy. Otherwise closes it, if open, and opens a new
   * one.
   *
   * This must strictly only be called after acquiring ALL permits from the semaphore. Otherwise, we
   * would open or close the resource while someone is still using it.
   */
  private def doReplace: F[Opened[F, A]] =
    ref.get.flatMap {
      // Another fiber replaced it while this fiber was waiting for all the permits
      case o @ Opened(_, _, _, false) => Sync[F].pure(o)
      case _ =>
        doClose *> Sync[F].uncancelable { poll =>
          for {
            token <- Unique[F].unique
            (a, close) <- poll(resource.allocated)
            o = Opened(a, close, token, broken = false)
            _ <- ref.set(o)
          } yield o
        }
    }

}

object Coldswap {

  /**
   * A resource handed out by `Coldswap.opened`, valid for the duration of the `use` block.
   *
   * `invalidate` marks this resource as broken so that it is replaced once every current holder has
   * released it. It has no effect if the resource was already replaced.
   */
  sealed trait Lease[F[_], A] {
    def value: A
    def invalidate: F[Unit]
  }

  private sealed trait State[+F[_], +A]
  private case object Closed extends State[Nothing, Nothing]
  private case class Opened[F[_], A](
    value: A,
    close: F[Unit],
    token: Unique.Token,
    broken: Boolean
  ) extends State[F, A]

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def make[F[_]: Async, A](resource: Resource[F, A]): Resource[F, Coldswap[F, A]] =
    for {
      sem <- Resource.eval(Semaphore[F](Long.MaxValue))
      ref <- Resource.eval(Ref.of[F, State[F, A]](Closed))
      coldswap = new Coldswap(sem, ref, resource)
      _ <- Resource.onFinalize(coldswap.shutdown)
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
