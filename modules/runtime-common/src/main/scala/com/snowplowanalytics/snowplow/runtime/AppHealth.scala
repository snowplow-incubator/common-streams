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

import cats.{Eq, Show}
import cats.Monad
import cats.implicits._
import cats.effect.{Async, Ref}
import fs2.concurrent.SignallingRef

/**
 * Class to collect, store and provide the health statuses of services required by the App
 *
 * @tparam SetupAlert
 *   Sealed trait of the alerts this app is allowed to send to the webhook for setup errors
 * @tparam RuntimeService
 *   Sealed trait of the services that this app requires to be healthy
 *
 * ==App Boilerplate==
 *
 * To use this class in a Snowplow app, first enumerate the runtime services required by this app:
 *
 * {{{
 * trait MyAppRuntimeService
 * case object BadRowsOutputStream extends MyAppRuntimeService
 * case object IgluClient extends MyAppRuntimeService
 * }}}
 *
 * Next, define the alerts this app is allowed to send to the webhook for setup errors
 *
 * {{{
 * trait MyAppAlert
 * case object BadPassword extends MyAppAlert
 * case class MissingPermissions(moreDetails: String) extends MyAppAlert
 * }}}
 *
 * Implement a `cats.Show` for the runtime services and for the alerts
 *
 * {{{
 * val runtimeShow = Show[MyAppRuntimeService] {
 *   case BadRowsOutputStream => "Bad rows output stream"
 *   case IgluClient => "Iglu client"
 * }
 *
 * val alertShow = Show[MyAppAlert] {
 *   case BadPassword => "Bad password"
 *   case MissingPermissions(moreDetails) => "Missing permissions " + moreDetails
 * }
 * }}}
 *
 * ==Environment initialization==
 *
 * Initialize the AppHealth as part of App Environment initialization:
 *
 * {{{
 * appHealth <- AppHealth.init[F, MyAppAlert, MyAppRuntimeService]
 * }}}
 *
 * Initialize a health probe, so the app reports itself as unhealthy whenever a required service
 * becomes unhealthy
 *
 * {{{
 * _ <- HealthProbe.resource(port, appHealthy)
 * }}}
 *
 * Initialize the webhook, so the app reports new setup errors or whenever the setup configuration
 * becomes healthy
 *
 * {{{
 * _ <- Webhook.resource(config, appInfo, httpClient, appHealth)
 * }}}
 *
 * And finally, register any runtime service that provides its own health reporter. Not all services
 * fall into this category, but the source stream does fall into this category.
 *
 * {{{
 * _ <- appHealth.addRuntimeHealthReporter(sourceAndAckIsHealthy)
 * }}}
 *
 * ==Application processing==
 *
 * Once the application enters its processing phase, you can set the health status for any runtime
 * service or setup configuration
 *
 * {{{
 * // After catching an exception in the bad events stream
 * _ <- Logger[F].error(e)("Problem with bad rows output stream")
 * _ <- appHealth.beUnhealthyForRuntimeService(BadRowsOutputStream)
 *
 * // After bad rows stream becomes healthy again
 * _ <- Logger[F].debug("Bad rows output stream is ok")
 * _ <- appHealth.beHealthyForRuntimeService(BadRowsOutputStream)
 *
 * // After catching an exception with the external setup configuration
 * // Note this will send an alert webhook
 * _ <- Logger[F].error(e)("Problem with the provided password")
 * _ <- appHealth.beUnhealthyForSetup(BadPassword)
 *
 * // After successful connection to the externally configured services
 * // Note this will send the first hearbeat webhook
 * _ <- Logger[F].error(e)("Everything ok with the provided setup configuration")
 * _ <- appHealth.beHealthyForSetup
 * }}}
 *
 * The application processing code does not need to explicitly send any monitoring alert or adjust
 * the health probe return code.
 */
class AppHealth[F[_]: Monad, SetupAlert, RuntimeService] private (
  private[runtime] val setupHealth: SignallingRef[F, AppHealth.SetupStatus[SetupAlert]],
  unhealthyRuntimeServices: Ref[F, Set[RuntimeService]],
  runtimeServiceReporters: List[F[Option[String]]]
) extends AppHealth.Interface[F, SetupAlert, RuntimeService] {
  import AppHealth._

  def beHealthyForSetup: F[Unit] =
    setupHealth.set(SetupStatus.Healthy)

  def beUnhealthyForSetup(alert: SetupAlert): F[Unit] =
    setupHealth.set(SetupStatus.Unhealthy(alert))

  def beHealthyForRuntimeService(service: RuntimeService): F[Unit] =
    unhealthyRuntimeServices.update(_ - service)

  def beUnhealthyForRuntimeService(service: RuntimeService): F[Unit] =
    unhealthyRuntimeServices.update(_ + service)

  private[runtime] def unhealthyRuntimeServiceMessages(implicit show: Show[RuntimeService]): F[List[String]] =
    for {
      services <- unhealthyRuntimeServices.get
      extras <- runtimeServiceReporters.sequence
    } yield services.toList.map(_.show) ::: extras.flatten
}

object AppHealth {

  /**
   * Subset of AppHealth interface required during app's event-processing phase
   *
   * This interface should be part of the App's Environment. Processing Specs can provide a custom
   * implementation.
   */
  trait Interface[F[_], -SetupAlert, -RuntimeService] {
    def beHealthyForSetup: F[Unit]

    def beUnhealthyForSetup(alert: SetupAlert): F[Unit]

    def beHealthyForRuntimeService(service: RuntimeService): F[Unit]

    def beUnhealthyForRuntimeService(service: RuntimeService): F[Unit]
  }

  private[runtime] sealed trait SetupStatus[+Alert]
  private[runtime] object SetupStatus {
    case object AwaitingHealth extends SetupStatus[Nothing]
    case object Healthy extends SetupStatus[Nothing]
    case class Unhealthy[Alert](alert: Alert) extends SetupStatus[Alert]

    implicit def eq[Alert: Show]: Eq[SetupStatus[Alert]] = Eq.instance {
      case (Healthy, Healthy)               => true
      case (Healthy, _)                     => false
      case (AwaitingHealth, AwaitingHealth) => true
      case (AwaitingHealth, _)              => false
      case (Unhealthy(a1), Unhealthy(a2))   => a1.show === a2.show
      case (Unhealthy(_), _)                => false
    }
  }

  /**
   * Initialize the AppHealth
   *
   * @tparam SetupAlert
   *   Sealed trait of the alerts this app is allowed to send to the webhook for setup errors
   * @tparam RuntimeService
   *   Sealed trait of the services that this app requires to be healthy
   *
   * @param runtimeReporters
   *   Reporters for any additional service, not covered by `RuntimeService`. Reporters provide a
   *   String if a service is unhealthy. The String must be a short description of why the service
   *   is unhealthy.
   */
  def init[F[_]: Async, SetupAlert, RuntimeService](
    runtimeReporters: List[F[Option[String]]]
  ): F[AppHealth[F, SetupAlert, RuntimeService]] =
    for {
      setupHealth <- SignallingRef[F, SetupStatus[SetupAlert]](SetupStatus.AwaitingHealth)
      unhealthyRuntimeServices <- Ref[F].of(Set.empty[RuntimeService])
    } yield new AppHealth(setupHealth, unhealthyRuntimeServices, runtimeReporters)
}
