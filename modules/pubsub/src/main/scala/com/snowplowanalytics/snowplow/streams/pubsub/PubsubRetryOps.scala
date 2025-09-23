/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import cats.MonadError
import cats.implicits._
import cats.effect.{Async, Poll}
import com.google.api.gax.rpc.{ApiException, StatusCode}
import io.grpc.Status
import org.typelevel.log4cats.Logger
import retry.{RetryPolicies, Sleep}
import retry.implicits._

import scala.concurrent.duration.FiniteDuration

private[pubsub] object PubsubRetryOps {

  object implicits {
    implicit class Ops[F[_], A](val f: F[A]) extends AnyVal {

      /**
       * Retries an effect when an exception matches known retryable GRPC errors
       *
       * The retry policy is full jitter, with capped number of attempts
       */
      def retryingOnTransientGrpcFailures(retryDelay: FiniteDuration, retryAttempts: Int)(implicit F: Async[F], L: Logger[F]): F[A] =
        retryingOnTransientGrpcFailuresImpl(retryDelay, retryAttempts)

      /**
       * Retries an effect when an exception matches known retryable GRPC errors
       *
       * The retry policy is full jitter, with capped number of attempts
       *
       * This variant takes a `Poll[F]` as a parameter. This is helpful when the effect is run
       * inside `Sync[F].uncancelable` but we don't mind if the effect is cancelled in between retry
       * attempts.
       */
      def retryingOnTransientGrpcFailuresWithCancelableSleep(
        retryDelay: FiniteDuration,
        retryAttempts: Int,
        poll: Poll[F]
      )(implicit F: Async[F],
        L: Logger[F]
      ): F[A] = {
        implicit val s: Sleep[F] = (delay: FiniteDuration) => poll(Async[F].sleep(delay))
        retryingOnTransientGrpcFailuresImpl(retryDelay, retryAttempts)
      }

      private def retryingOnTransientGrpcFailuresImpl(
        retryDelay: FiniteDuration,
        retryAttempts: Int
      )(implicit F: MonadError[F, Throwable],
        L: Logger[F],
        s: Sleep[F]
      ): F[A] =
        f.retryingOnSomeErrors(
          isWorthRetrying = { e => isRetryableException(e).pure[F] },
          policy          = RetryPolicies.fullJitter[F](retryDelay).join(RetryPolicies.limitRetries(retryAttempts - 1)),
          onError = { case (t, _) =>
            Logger[F].info(t)(s"Pubsub retryable GRPC error will be retried: ${t.getMessage}")
          }
        )

      def recoveringOnGrpcInvalidArgument(f2: Status => F[A])(implicit F: Async[F]): F[A] =
        f.recoverWith {
          case StatusFromThrowable(s) if s.getCode.equals(Status.Code.INVALID_ARGUMENT) =>
            f2(s)
        }
    }
  }

  private object StatusFromThrowable {
    def unapply(t: Throwable): Option[Status] =
      Some(Status.fromThrowable(t))
  }

  def isRetryableException: Throwable => Boolean = {
    case apiException: ApiException =>
      apiException.getStatusCode.getCode match {
        case StatusCode.Code.DEADLINE_EXCEEDED  => true
        case StatusCode.Code.INTERNAL           => true
        case StatusCode.Code.CANCELLED          => true
        case StatusCode.Code.RESOURCE_EXHAUSTED => true
        case StatusCode.Code.ABORTED            => true
        case StatusCode.Code.UNKNOWN            => true
        case StatusCode.Code.UNAVAILABLE        => !apiException.getMessage().contains("Server shutdownNow invoked")
        case _                                  => false
      }
    case _ =>
      false
  }
}
