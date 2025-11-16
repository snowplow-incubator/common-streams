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

import cats.Show
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{IO, Ref}
import org.specs2.Specification
import scala.concurrent.duration._

class RetryingSpec extends Specification with CatsEffect {
  import RetryingSpec._

  def is = s2"""
  The Retrying.withRetries should:
    Pass attempt number 0 on first execution $test1
    Pass attempt number 1 after first transient retry $test2
    Pass attempt number 2 after second transient retry $test3
    Pass incremented attempt numbers through multiple retries $test4
    Pass incremented attempt numbers on setup errors $test5
    Track attempts correctly across both retry strategies $test6
  """

  def test1 = {
    val config = createConfig()
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = PartialFunction.empty
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *> IO.pure("success")
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success")) and (attempts should beEqualTo(List(0)))
  }

  def test2 = {
    val config = createConfig()
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      callCount <- Ref[IO].of(0)
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = PartialFunction.empty
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *>
                    callCount.getAndUpdate(_ + 1).flatMap { count =>
                      if (count == 0) IO.raiseError(new RuntimeException("transient error"))
                      else IO.pure("success")
                    }
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success")) and (attempts should beEqualTo(List(0, 1)))
  }

  def test3 = {
    val config = createConfig()
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      callCount <- Ref[IO].of(0)
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = PartialFunction.empty
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *>
                    callCount.getAndUpdate(_ + 1).flatMap { count =>
                      if (count < 2) IO.raiseError(new RuntimeException("transient error"))
                      else IO.pure("success")
                    }
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success")) and (attempts should beEqualTo(List(0, 1, 2)))
  }

  def test4 = {
    val config = createConfig(maxAttempts = 5)
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      callCount <- Ref[IO].of(0)
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = PartialFunction.empty
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *>
                    callCount.getAndUpdate(_ + 1).flatMap { count =>
                      if (count < 3) IO.raiseError(new RuntimeException("transient error"))
                      else IO.pure("success after 3 retries")
                    }
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success after 3 retries")) and (attempts should beEqualTo(List(0, 1, 2, 3)))
  }

  def test5 = {
    val config = createConfig()
    val setupErrorCheck: PartialFunction[Throwable, String] = { case e: IllegalArgumentException =>
      e.getMessage
    }
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      callCount <- Ref[IO].of(0)
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = setupErrorCheck
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *>
                    callCount.getAndUpdate(_ + 1).flatMap { count =>
                      if (count == 0) IO.raiseError(new IllegalArgumentException("setup error"))
                      else IO.pure("success")
                    }
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success")) and (attempts should beEqualTo(List(0, 1)))
  }

  def test6 = {
    val config = createConfig()
    val setupErrorCheck: PartialFunction[Throwable, String] = { case e: IllegalArgumentException =>
      e.getMessage
    }
    for {
      appHealth <- AppHealth.init[IO, TestAlert, TestService](Nil)
      attemptsRef <- Ref[IO].of(List.empty[Int])
      callCount <- Ref[IO].of(0)
      result <- Retrying.withRetries[IO, TestAlert, TestService, String](
                  appHealth          = appHealth,
                  configForTransient = config.transient,
                  configForSetup     = config.setup,
                  service            = TestService1,
                  toAlert            = _ => TestAlert1,
                  setupErrorCheck    = setupErrorCheck
                ) { attempt =>
                  attemptsRef.update(_ :+ attempt) *>
                    callCount.getAndUpdate(_ + 1).flatMap {
                      case 0 => IO.raiseError(new IllegalArgumentException("setup error"))
                      case 1 => IO.raiseError(new RuntimeException("transient error"))
                      case _ => IO.pure("success")
                    }
                }
      attempts <- attemptsRef.get
    } yield (result should beEqualTo("success")) and (attempts should beEqualTo(List(0, 1, 2)))
  }
}

object RetryingSpec {

  sealed trait TestAlert
  case object TestAlert1 extends TestAlert

  sealed trait TestService
  case object TestService1 extends TestService

  implicit def showTestService: Show[TestService] = Show { case TestService1 =>
    "test service 1"
  }

  case class TestConfig(
    transient: Retrying.Config.ForTransient,
    setup: Retrying.Config.ForSetup
  )

  def createConfig(
    maxAttempts: Int               = 5,
    transientDelay: FiniteDuration = 1.millis,
    setupDelay: FiniteDuration     = 1.millis
  ): TestConfig =
    TestConfig(
      transient = Retrying.Config.ForTransient(delay = transientDelay, attempts = maxAttempts),
      setup     = Retrying.Config.ForSetup(delay = setupDelay)
    )
}
