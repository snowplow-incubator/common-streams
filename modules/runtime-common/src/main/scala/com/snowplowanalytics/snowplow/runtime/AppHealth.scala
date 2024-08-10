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

import cats.Eq
import cats.{Applicative, Monad}
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
 * case object EventsInputStream extends MyAppRuntimeService
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
 *   case EventsInputStream => "Events input stream"
 * }
 *
 * val alertShow = Show[MyAppAlert] {
 *   case BadPassword => "Bad password"
 *   case MissingPermissions(moreDetails) => s"Missing permissions $moreDetails"
 * }
 * }}}
 *
 * Implement a `cats.Eq` for the alerts, so we can alert on anything uniquely new
 * {{{
 * val alertEq = Eq.fromUniversalEquals[Alert] // Do it better than this
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
 * _ <- appHealth.addRuntimeHealthReporter(EventsInputStream, sourceAndAckIsHealthy)
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
 * _ <- appHealth.becomeUnhealthyForRuntimeService(BadRowsOutputStream)
 *
 * // After bad rows stream becomes healthy again
 * _ <- Logger[F].debug("Bad rows output stream is ok")
 * _ <- appHealth.becomeHealthyForRuntimeService(BadRowsOutputStream)
 *
 * // After catching an exception with the external setup configuration
 * // Note this will send an alert webhook
 * _ <- Logger[F].error(e)("Problem with the provided password")
 * _ <- appHealth.becomeUnhealthyForSetup(BadPassword)
 *
 * // After successful connection to the externally configured services
 * // Note this will send the first hearbeat webhook
 * _ <- Logger[F].error(e)("Everything ok with the provided setup configuration")
 * _ <- appHealth.becomeHealthyForSetup
 * }}}
 *
 * The application processing code does not need to explicitly send any monitoring alert or adjust
 * the health probe return code.
 */
class AppHealth[F[_]: Monad, SetupAlert, RuntimeService] private (
  private[runtime] val setupHealth: SignallingRef[F, AppHealth.SetupStatus[SetupAlert]],
  runtimeHealth: Ref[F, Map[RuntimeService, F[AppHealth.RuntimeServiceStatus]]]
) extends AppHealth.Interface[F, SetupAlert, RuntimeService] {
  import AppHealth._

  def becomeHealthyForSetup: F[Unit] =
    setupHealth.set(SetupStatus.Healthy)

  def becomeUnhealthyForSetup(alert: SetupAlert): F[Unit] =
    setupHealth.set(SetupStatus.Unhealthy(alert))

  def becomeHealthyForRuntimeService(service: RuntimeService): F[Unit] =
    runtimeHealth.update(_ - service)

  def becomeUnhealthyForRuntimeService(service: RuntimeService): F[Unit] =
    runtimeHealth.update(_ + (service -> Applicative[F].pure(RuntimeServiceStatus.Unhealthy)))

  def addRuntimeHealthReporter(service: RuntimeService, reporter: F[RuntimeServiceStatus]): F[Unit] =
    runtimeHealth.update(_ + (service -> reporter))

  private[runtime] def unhealthyRuntimeServices: F[List[RuntimeService]] =
    for {
      asMap <- runtimeHealth.get
      pairs <- asMap.toList.traverse { case (service, statusF) =>
                 statusF.map((_, service))
               }
    } yield pairs.collect { case (RuntimeServiceStatus.Unhealthy, service) => service }
}

object AppHealth {

  /**
   * Subset of AppHealth interface required during app's event-processing phase
   *
   * This interface should be part of the App's Environment. Processing Specs can provide a custom
   * implementation.
   */
  trait Interface[F[_], -SetupAlert, -RuntimeService] {
    def becomeHealthyForSetup: F[Unit]

    def becomeUnhealthyForSetup(alert: SetupAlert): F[Unit]

    def becomeHealthyForRuntimeService(service: RuntimeService): F[Unit]

    def becomeUnhealthyForRuntimeService(service: RuntimeService): F[Unit]
  }

  private[runtime] sealed trait SetupStatus[+Alert]
  private[runtime] object SetupStatus {
    case object AwaitingHealth extends SetupStatus[Nothing]
    case object Healthy extends SetupStatus[Nothing]
    case class Unhealthy[Alert](alert: Alert) extends SetupStatus[Alert]

    implicit def eq[Alert: Eq]: Eq[SetupStatus[Alert]] = Eq.instance {
      case (Healthy, Healthy)               => true
      case (Healthy, _)                     => false
      case (AwaitingHealth, AwaitingHealth) => true
      case (AwaitingHealth, _)              => false
      case (Unhealthy(a1), Unhealthy(a2))   => Eq[Alert].eqv(a1, a2)
      case (Unhealthy(_), _)                => false
    }
  }

  sealed trait RuntimeServiceStatus
  object RuntimeServiceStatus {
    case object Healthy extends RuntimeServiceStatus
    case object Unhealthy extends RuntimeServiceStatus
  }

  /**
   * Initialize the AppHealth
   *
   * @tparam SetupAlert
   *   Sealed trait of the alerts this app is allowed to send to the webhook for setup errors
   * @tparam RuntimeService
   *   Sealed trait of the services that this app requires to be healthy
   */
  def init[F[_]: Async, SetupAlert, RuntimeService]: F[AppHealth[F, SetupAlert, RuntimeService]] =
    for {
      setupHealth <- SignallingRef[F, SetupStatus[SetupAlert]](SetupStatus.AwaitingHealth)
      runtimeHealth <- Ref[F].of(Map.empty[RuntimeService, F[AppHealth.RuntimeServiceStatus]])
    } yield new AppHealth(setupHealth, runtimeHealth)
}
