/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.testing.specs2.CatsEffect
import cats.effect.{IO, Ref}
import org.specs2.Specification

class AppHealthSpec extends Specification with CatsEffect {
  import AppHealthSpec._

  def is = s2"""
  The AppHealth should:
    For Runtime health:
      Start healthy $runtime1
      Report one unhealthy service if one service is unhealthy $runtime2
      Report two unhealthy services if two services are unhealthy $runtime3
      Become unhealthy after one service recovers $runtime4
      Report one unhealthy service if two services were unhealthy and one recovers $runtime5
      Report healthy status for an external reporter $runtime6
    For Setup health:
      Start with status of awaiting health $setup1
      Report unhealthy after told of a setup problem $setup2
      Report healthy after told of a healthy setup $setup3
      Recover from an unhealthy status when told $setup4
      Return to an unhealthy status when told $setup5
  """

  def runtime1 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    statuses <- appHealth.unhealthyRuntimeServices
  } yield statuses should beEmpty

  def runtime2 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    statuses <- appHealth.unhealthyRuntimeServices
  } yield statuses should beEqualTo(List(TestService1))

  def runtime3 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService2)
    statuses <- appHealth.unhealthyRuntimeServices
  } yield statuses should containTheSameElementsAs(List(TestService1, TestService2))

  def runtime4 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    _ <- appHealth.becomeHealthyForRuntimeService(TestService1)
    statuses <- appHealth.unhealthyRuntimeServices
  } yield statuses should beEmpty

  def runtime5 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService1)
    _ <- appHealth.becomeUnhealthyForRuntimeService(TestService2)
    _ <- appHealth.becomeHealthyForRuntimeService(TestService1)
    statuses <- appHealth.unhealthyRuntimeServices
  } yield statuses should beEqualTo(List(TestService2))

  def runtime6 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    reporter <- Ref[IO].of[AppHealth.RuntimeServiceStatus](AppHealth.RuntimeServiceStatus.Healthy)
    _ <- appHealth.addRuntimeHealthReporter(TestService1, reporter.get)
    result1 <- appHealth.unhealthyRuntimeServices
    _ <- reporter.set(AppHealth.RuntimeServiceStatus.Unhealthy)
    result2 <- appHealth.unhealthyRuntimeServices
    _ <- reporter.set(AppHealth.RuntimeServiceStatus.Healthy)
    result3 <- appHealth.unhealthyRuntimeServices
  } yield (result1 should beEmpty) and (result2 should beEqualTo(List(TestService1))) and (result3 should beEmpty)

  def setup1 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    setupHealth <- appHealth.setupHealth.get
  } yield setupHealth should beEqualTo(AppHealth.SetupStatus.AwaitingHealth)

  def setup2 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForSetup(TestAlert1)
    setupHealth <- appHealth.setupHealth.get
  } yield setupHealth should beEqualTo(AppHealth.SetupStatus.Unhealthy(TestAlert1))

  def setup3 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeHealthyForSetup
    setupHealth <- appHealth.setupHealth.get
  } yield setupHealth should beEqualTo(AppHealth.SetupStatus.Healthy)

  def setup4 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeUnhealthyForSetup(TestAlert1)
    _ <- appHealth.becomeHealthyForSetup
    setupHealth <- appHealth.setupHealth.get
  } yield setupHealth should beEqualTo(AppHealth.SetupStatus.Healthy)

  def setup5 = for {
    appHealth <- AppHealth.init[IO, TestAlert, TestService]
    _ <- appHealth.becomeHealthyForSetup
    _ <- appHealth.becomeUnhealthyForSetup(TestAlert1)
    setupHealth <- appHealth.setupHealth.get
  } yield setupHealth should beEqualTo(AppHealth.SetupStatus.Unhealthy(TestAlert1))

}

object AppHealthSpec {

  sealed trait TestAlert
  case object TestAlert1 extends TestAlert
  case object TestAlert2 extends TestAlert

  sealed trait TestService
  case object TestService1 extends TestService
  case object TestService2 extends TestService
}
