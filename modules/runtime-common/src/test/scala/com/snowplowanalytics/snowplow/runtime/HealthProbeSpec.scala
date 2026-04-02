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
import org.http4s.{Method, Request, Status, Uri}
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
  The /metrics endpoint should:
    Return 200 with scraped metrics body $metrics1
    Return 200 with updated metrics on subsequent scrapes $metrics2
  """

  def probe1 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure(""))
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe2 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure(""))
    _ <- appHealth.beHealthyForSetup
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.Ok)

  def probe3 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure(""))
    _ <- appHealth.beHealthyForSetup
    _ <- appHealth.beUnhealthyForSetup(TestAlert1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe4 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure(""))
    _ <- appHealth.beHealthyForSetup
    _ <- appHealth.beUnhealthyForRuntimeService(TestService1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.ServiceUnavailable)

  def probe5 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure(""))
    _ <- appHealth.beHealthyForSetup
    _ <- appHealth.beUnhealthyForRuntimeService(TestService1)
    _ <- appHealth.beHealthyForRuntimeService(TestService1)
    response <- httpApp.run(Request[IO]())
  } yield response.status must beEqualTo(Status.Ok)

  def metrics1 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, IO.pure("my_counter_total 42.0"))
    response <- httpApp.run(Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/metrics")))
    body <- response.bodyText.compile.string
  } yield List(
    response.status must beEqualTo(Status.Ok),
    body must contain("my_counter_total 42.0")
  ).reduce(_ and _)

  def metrics2 = for {
    ref <- IO.ref("scrape_1")
    appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
    httpApp = HealthProbe.httpApp(appHealth, ref.get)
    response1 <- httpApp.run(Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/metrics")))
    body1 <- response1.bodyText.compile.string
    _ <- ref.set("scrape_2")
    response2 <- httpApp.run(Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/metrics")))
    body2 <- response2.bodyText.compile.string
  } yield List(
    body1 must contain("scrape_1"),
    body2 must contain("scrape_2")
  ).reduce(_ and _)

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
