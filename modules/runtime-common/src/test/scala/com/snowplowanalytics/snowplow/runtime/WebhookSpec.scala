/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.{Applicative, Id, Show}
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{Clock, IO, Ref, Resource}
import cats.effect.testkit.TestControl
import org.http4s.{Headers, Method, Response}
import org.http4s.client.Client
import io.circe.Json
import io.circe.parser.{parse => circeParse}
import org.http4s.Uri
import org.specs2.Specification
import org.specs2.matcher.Matcher

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import java.util.concurrent.TimeUnit

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup.idLookupInstance

class WebhookSpec extends Specification with CatsEffect {
  import WebhookSpec._

  def is = s2"""
  The webhook should:
    Not send any payloads if app health never leaves awaiting status $send1
    Send a single heartbeat after app becomes healthy for setup $send2
    Send a second heartbeat after configured period of time $send3
    Send a single alert after app becomes unhealthy for setup $send4
    Send multiple alerts if app becomes unhealthy for setup with different alert messages $send5
    Send alternating hearbeat and alert if app health flip flops $send6
    Not send any payloads if endpoint is not set in the configuration $send7
    Ignore any exception raised by sending webhook $send8
  """

  def send1 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      val _ = appHealth
      for {
        _ <- IO.sleep(60.minutes)
        reportedRequests <- getReportedRequests
      } yield reportedRequests should beEmpty
    }
    TestControl.executeEmbed(io)
  }

  def send2 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        reportedRequests <- getReportedRequests
      } yield List(
        reportedRequests should haveSize(1),
        reportedRequests must contain(beValidHeartbeatRequest)
      ).reduce(_ and _)
    }
    TestControl.executeEmbed(io)
  }

  def send3 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(45.minutes)
        reportedRequests <- getReportedRequests
      } yield List(
        reportedRequests should haveSize(2),
        reportedRequests must contain(beValidHeartbeatRequest).forall
      ).reduce(_ and _)
    }
    TestControl.executeEmbed(io)
  }

  def send4 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom!"))
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom!"))
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom!"))
        _ <- IO.sleep(5.minutes)
        reportedRequests <- getReportedRequests
      } yield List(
        reportedRequests should haveSize(1),
        reportedRequests must contain(beValidAlertRequest)
      ).reduce(_ and _)
    }
    TestControl.executeEmbed(io)
  }

  def send5 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom 1"))
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom 2"))
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom 3"))
        _ <- IO.sleep(5.minutes)
        reportedRequests <- getReportedRequests
      } yield List(
        reportedRequests should haveSize(3),
        reportedRequests must contain(beValidAlertRequest).forall
      ).reduce(_ and _)
    }
    TestControl.executeEmbed(io)
  }

  def send6 = {
    val io = resources().use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom!"))
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beUnhealthyForSetup(TestAlert("boom!"))
        _ <- IO.sleep(5.minutes)
        reportedRequests <- getReportedRequests
      } yield List(
        reportedRequests should haveSize(4),
        reportedRequests must contain(beValidAlertRequest).exactly(2.times),
        reportedRequests must contain(beValidHeartbeatRequest).exactly(2.times)
      ).reduce(_ and _)
    }
    TestControl.executeEmbed(io)
  }

  def send7 = {
    val config = testConfig.copy(endpoint = None)
    val io = resources(config).use { case (getReportedRequests, appHealth) =>
      for {
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(5.minutes)
        reportedRequests <- getReportedRequests
      } yield reportedRequests should beEmpty
    }
    TestControl.executeEmbed(io)
  }

  def send8 = {
    val resources = for {
      appHealth <- Resource.eval(AppHealth.init[IO, TestAlert, TestService](Nil))
      _ <- Webhook.resource(testConfig, testAppInfo, errorRaisingHttpClient, appHealth)
    } yield appHealth

    val io = resources.use { appHealth =>
      for {
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(30.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(30.minutes)
        _ <- appHealth.beHealthyForSetup
        _ <- IO.sleep(30.minutes)
      } yield ok
    }

    TestControl.executeEmbed(io)
  }

  private def beValidAlertRequest: Matcher[ReportedRequest] = { (req: ReportedRequest) =>
    List(
      mustHaveValidAlertBody(req.body),
      req.method must beEqualTo(Method.POST),
      req.uri must beEqualTo(testEndpoint),
      req.headers.toString must contain("Content-Type: application/json")
    ).reduce(_ and _)
  }

  private def beValidHeartbeatRequest: Matcher[ReportedRequest] = { (req: ReportedRequest) =>
    List(
      mustHaveValidHeartbeatBody(req.body),
      req.method must beEqualTo(Method.POST),
      req.uri must beEqualTo(testEndpoint),
      req.headers.toString must contain("Content-Type: application/json")
    ).reduce(_ and _)
  }

  private def mustHaveValidAlertBody =
    mustHaveValidSdjBody("iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0") _

  private def mustHaveValidHeartbeatBody =
    mustHaveValidSdjBody("iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0") _

  private def mustHaveValidSdjBody(igluUri: String)(body: String) =
    circeParse(body) must beRight.like { case json: Json =>
      json.as[SelfDescribingData[Json]] must beRight.like { case sdj: SelfDescribingData[Json] =>
        List(
          sdj.schema.toSchemaUri must beEqualTo(igluUri),
          igluCirceClient.check(sdj).value must beRight
        ).reduce(_ and _)
      }
    }

}

object WebhookSpec {

  case class TestAlert(msg: String)

  implicit def testAlertShow: Show[TestAlert] =
    Show(_.msg)

  sealed trait TestService
  case object TestService1 extends TestService
  case object TestService2 extends TestService

  val testAppInfo: AppInfo = new AppInfo {
    def name: String        = "testName"
    def version: String     = "testVersion"
    def dockerAlias: String = "testDockerAlias"
    def cloud: String       = "testCloud"
  }

  def testEndpoint = Uri.unsafeFromString("http://example.com/xyz?abc=123")

  def testConfig: Webhook.Config = Webhook.Config(
    endpoint  = Some(testEndpoint),
    tags      = Map("myTag" -> "myValue"),
    heartbeat = 42.minutes
  )

  // Used in tests to report the request that was sent to the webhook
  case class ReportedRequest(
    method: Method,
    uri: Uri,
    headers: Headers,
    body: String
  )

  // A http4s Client that reports what requests it has sent
  def reportingHttpClient(ref: Ref[IO, List[ReportedRequest]]): Client[IO] =
    Client[IO] { request =>
      Resource.eval {
        for {
          body <- request.bodyText.compile.string
          _ <- ref.update(_ :+ ReportedRequest(request.method, request.uri, request.headers, body))
        } yield Response.notFound[IO]
      }
    }

  /**
   * Resources for running a Spec
   *
   * @return
   *   a IO that records the requests sent to the webhook, and the AppHealth on which the spec can
   *   set healthy/unhealthy services
   */
  def resources(
    config: Webhook.Config = testConfig
  ): Resource[IO, (IO[List[ReportedRequest]], AppHealth.Interface[IO, TestAlert, TestService])] = for {
    ref <- Resource.eval(Ref[IO].of(List.empty[ReportedRequest]))
    httpClient = reportingHttpClient(ref)
    appHealth <- Resource.eval(AppHealth.init[IO, TestAlert, TestService](Nil))
    _ <- Webhook.resource(config, testAppInfo, httpClient, appHealth)
  } yield (ref.get, appHealth)

  // A http4s Client that raises exceptions
  def errorRaisingHttpClient: Client[IO] =
    Client[IO] { _ =>
      Resource.raiseError[IO, Response[IO], Throwable](new RuntimeException("boom!"))
    }

  def igluCirceClient: IgluCirceClient[Id] =
    IgluCirceClient.fromResolver[Id](Resolver[Id](Nil, None), 0)

  // Needed because we use Id effect in tests for iglu-scala-client
  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def applicative: Applicative[Id] = Applicative[Id]

    override def monotonic: Id[FiniteDuration] = FiniteDuration(System.nanoTime(), TimeUnit.NANOSECONDS)

    override def realTime: Id[FiniteDuration] = FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
  }
}
