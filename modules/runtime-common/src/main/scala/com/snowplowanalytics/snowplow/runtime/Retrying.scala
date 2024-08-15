/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.runtime

import cats.{Applicative, Show}
import cats.effect.Sync
import cats.implicits._
import retry._
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._
import retry.implicits.retrySyntaxError
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.FiniteDuration

object Retrying {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  // Publicly accessible to help Snowplow apps that manage their own retrying
  implicit def showRetryDetails: Show[RetryDetails] = Show {
    case RetryDetails.GivingUp(totalRetries, totalDelay) =>
      s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
    case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
      s"Will retry in ${nextDelay.toMillis} milliseconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toMillis} milliseconds"
  }

  object Config {

    case class ForSetup(delay: FiniteDuration)
    object ForSetup {
      implicit def decoder: Decoder[ForSetup] =
        deriveDecoder
    }

    case class ForTransient(delay: FiniteDuration, attempts: Int)
    object ForTransient {
      implicit def decoder: Decoder[ForTransient] =
        deriveDecoder
    }
  }

  def withRetries[F[_]: Sync: Sleep, Alert, RuntimeService, A](
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    configForTransient: Config.ForTransient,
    configForSetup: Config.ForSetup,
    service: RuntimeService,
    toAlert: SetupExceptionMessages => Alert,
    setupErrorCheck: PartialFunction[Throwable, String]
  )(
    action: F[A]
  ): F[A] =
    action
      .retryingOnSomeErrors(
        isWorthRetrying = checkingNestedExceptions(setupErrorCheck, _).nonEmpty.pure[F],
        policy          = policyForSetupErrors[F](configForSetup),
        onError         = logErrorAndSendAlert(appHealth, setupErrorCheck, toAlert, _, _)
      )
      .retryingOnAllErrors(
        policy  = policyForTransientErrors[F](configForTransient),
        onError = logErrorAndReportUnhealthy(appHealth, service, _, _)
      )
      .productL(appHealth.beHealthyForRuntimeService(service))

  private def policyForSetupErrors[F[_]: Applicative](config: Config.ForSetup): RetryPolicy[F] =
    RetryPolicies.exponentialBackoff[F](config.delay)

  private def policyForTransientErrors[F[_]: Applicative](config: Config.ForTransient): RetryPolicy[F] =
    RetryPolicies.fullJitter[F](config.delay).join(RetryPolicies.limitRetries(config.attempts - 1))

  private def logErrorAndSendAlert[F[_]: Sync, Alert, RuntimeService](
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    setupErrorCheck: PartialFunction[Throwable, String],
    toAlert: SetupExceptionMessages => Alert,
    error: Throwable,
    details: RetryDetails
  ): F[Unit] =
    logError(error, details) *> appHealth.beUnhealthyForSetup(
      toAlert(SetupExceptionMessages(checkingNestedExceptions(setupErrorCheck, error)))
    )

  private def logError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    Logger[F].error(error)(show"Executing command failed. $details")

  private def logErrorAndReportUnhealthy[F[_]: Sync, RuntimeService](
    appHealth: AppHealth.Interface[F, ?, RuntimeService],
    service: RuntimeService,
    error: Throwable,
    details: RetryDetails
  ): F[Unit] =
    logError(error, details) *> appHealth.beUnhealthyForRuntimeService(service)

  // Returns a list of reasons of why this was a destination setup error.
  // Or empty list if this was not caused by a destination setup error
  private def checkingNestedExceptions(
    setupErrorCheck: PartialFunction[Throwable, String],
    t: Throwable
  ): List[String] =
    unnestThrowableCauses(t).map(setupErrorCheck.lift).flatten

  private def unnestThrowableCauses(t: Throwable): List[Throwable] = {
    def go(t: Throwable, acc: List[Throwable]): List[Throwable] =
      Option(t.getCause) match {
        case Some(cause) => go(cause, cause :: acc)
        case None        => acc.reverse
      }
    go(t, List(t))
  }
}
