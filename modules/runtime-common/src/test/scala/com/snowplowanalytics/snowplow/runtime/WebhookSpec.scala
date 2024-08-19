/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.{Applicative, Id, Show}
import cats.implicits._
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{Clock, IO, Ref, Resource}
import org.http4s.{Headers, Method, Response}
import org.http4s.client.Client
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.parser.{parse => circeParse}
import io.circe.DecodingFailure
import org.http4s.Uri
import org.specs2.Specification

import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup.idLookupInstance

class WebhookSpec extends Specification with CatsEffect {
  import WebhookSpec._

  def is = s2"""
  The webhook config decoder should:
    Decode a valid JSON config $decode1
    Not decode JSON if a required field is missing $decode2
  The webhook should:
    Not send any payloads if config is empty $send1
    Send a valid payload if given valid config $send2
    Ignore any exception raised by sending webhook $send3
  """

  def decode1 = {
    val json = json"""
    {
      "endpoint": "http://example.com/xyz?abc=123",
      "tags": {
        "abc": "xyz"
      }
    }
    """

    json.as[Option[Webhook.Config]] must beRight.like { case Some(c: Webhook.Config) =>
      List(
        c.endpoint must beEqualTo(Uri.unsafeFromString("http://example.com/xyz?abc=123")),
        c.tags must beEqualTo(Map("abc" -> "xyz"))
      ).reduce(_ and _)
    }
  }

  def decode2 = {
    val json = json"""
    {
      "tags": {
        "abc": "xyz"
      }
    }
    """

    json.as[Option[Webhook.Config]] must beLeft.like { case e: DecodingFailure =>
      e.show must beEqualTo("DecodingFailure at .endpoint: Missing required field")
    }
  }

  def send1 = for {
    ref <- Ref[IO].of(List.empty[ReportedRequest])
    httpClient = reportingHttpClient(ref)
    webhook    = Webhook.create[IO, TestAlert](None, testAppInfo, httpClient)
    _ <- webhook.alert(TestAlert("this is a test"))
    results <- ref.get
  } yield results must beEmpty

  def send2 = for {
    ref <- Ref[IO].of(List.empty[ReportedRequest])
    httpClient = reportingHttpClient(ref)
    webhook    = Webhook.create[IO, TestAlert](Some(testConfig), testAppInfo, httpClient)
    _ <- webhook.alert(TestAlert("this is a test"))
    results <- ref.get
  } yield List(
    results must haveSize(1),
    results must contain { req: ReportedRequest =>
      List(
        mustHaveValidAlertBody(req.body),
        req.method must beEqualTo(Method.POST),
        req.uri must beEqualTo(testConfig.endpoint),
        req.headers.toString must contain("Content-Type: application/json")
      ).reduce(_ and _)
    }
  ).reduce(_ and _)

  def send3 = {
    val webhook = Webhook.create[IO, TestAlert](Some(testConfig), testAppInfo, errorRaisingHttpClient)
    for {
      _ <- webhook.alert(TestAlert("this is a test"))
    } yield ok
  }

  private def mustHaveValidAlertBody(body: String) =
    circeParse(body) must beRight.like { case json: Json =>
      json.as[SelfDescribingData[Json]] must beRight.like { case sdj: SelfDescribingData[Json] =>
        List(
          sdj.schema.toSchemaUri must beEqualTo("iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0"),
          igluCirceClient.check(sdj).value must beRight
        ).reduce(_ and _)
      }
    }

}

object WebhookSpec {

  case class TestAlert(msg: String)

  implicit def testAlertShow: Show[TestAlert] =
    Show(_.msg)

  val testAppInfo: AppInfo = new AppInfo {
    def name: String        = "testName"
    def version: String     = "testVersion"
    def dockerAlias: String = "testDockerAlias"
    def cloud: String       = "testCloud"
  }

  def testConfig: Webhook.Config = Webhook.Config(
    endpoint = Uri.unsafeFromString("http://example.com/xyz?abc=123"),
    tags     = Map("myTag" -> "myValue")
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

  // A http4s Client that raises exceptions
  def errorRaisingHttpClient: Client[IO] =
    Client[IO] { _ =>
      Resource.raiseError[IO, Response[IO], Throwable](new RuntimeException("boom!"))
    }

  def igluCirceClient: IgluCirceClient[Id] =
    IgluCirceClient.fromResolver[Id](Resolver[Id](Nil, None), 0, 1000)

  // Needed because we use Id effect in tests for iglu-scala-client
  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def applicative: Applicative[Id] = Applicative[Id]

    override def monotonic: Id[FiniteDuration] = FiniteDuration(System.nanoTime(), TimeUnit.NANOSECONDS)

    override def realTime: Id[FiniteDuration] = FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
  }
}
