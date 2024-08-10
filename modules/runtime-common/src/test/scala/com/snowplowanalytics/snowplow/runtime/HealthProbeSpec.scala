/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.testing.specs2.CatsEffect
import cats.effect.IO
import cats.Show
import org.http4s.{Request, Status}
import org.specs2.Specification

class HealthProbeSpec extends Specification with CatsEffect {
  import HealthProbeSpec._

  def is = s2"""
  The HealthProbe should:
    Initially return 503 $probe1
    Return 200 after setup configuration is healthy $probe2
    Return 503 after setup configuration becomes unhealthy $probe3
    Return 503 if a runtime service is unhealthy $probe4
    Return 200 after a runtime service recovers $probe5
  """

  def probe1 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    httpApp = HealthProbe.httpApp(appHealth)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe2 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    httpApp = HealthProbe.httpApp(appHealth)
    _ <- appHealth.becomeHealthyForSetup
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.Ok)

  def probe3 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    httpApp = HealthProbe.httpApp(appHealth)
    _ <- appHealth.becomeHealthyForSetup
    _ <- appHealth.becomeUnhealthyForSetup(TestAlert1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe4 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    httpApp = HealthProbe.httpApp(appHealth)
    _ <- appHealth.becomeHealthyForSetup
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe5 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    httpApp = HealthProbe.httpApp(appHealth)
    _ <- appHealth.becomeHealthyForSetup
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    _ <- appHealth.becomeHealthyForRuntimeService(TestService1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.Ok)

}

object HealthProbeSpec {

  sealed trait TestAlert
  case object TestAlert1 extends TestAlert
  case object TestAlert2 extends TestAlert

  sealed trait TestService
  case object TestService1 extends TestService
  case object TestService2 extends TestService

  implicit def showTestAlert: Show[TestService] = Show[TestService](_.toString)
}
