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

import cats.effect.{Async, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import cats.Show
import fs2.{Pipe, Pull, Stream}
import io.circe.{Decoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.config.syntax._
import org.http4s.circe.jsonEncoder
import org.http4s.client.Client
import org.http4s.{Method, Request}
import org.http4s.{ParseFailure, Uri}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

object Webhook {

  final case class Config(
    endpoint: Option[Uri],
    tags: Map[String, String],
    heartbeat: FiniteDuration
  )

  object Config {
    implicit def webhookConfigDecoder: Decoder[Config] = {
      implicit val http4sUriDecoder: Decoder[Uri] =
        Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))
      deriveDecoder[Config]
    }
  }

  def resource[F[_]: Async, SetupAlert: Show](
    config: Config,
    appInfo: AppInfo,
    httpClient: Client[F],
    appHealth: AppHealth[F, SetupAlert, ?]
  ): Resource[F, Unit] =
    stream(config, appInfo, httpClient, appHealth).compile.drain.background.void

  def stream[F[_]: Async, SetupAlert: Show](
    config: Config,
    appInfo: AppInfo,
    httpClient: Client[F],
    appHealth: AppHealth[F, SetupAlert, ?]
  ): Stream[F, Nothing] =
    appHealth.setupHealth.discrete.changes
      .through(repeatPeriodically(config.heartbeat))
      .flatMap {
        case AppHealth.SetupStatus.AwaitingHealth =>
          Stream.empty
        case AppHealth.SetupStatus.Healthy =>
          buildHeartbeatHttpRequest[F](config, appInfo)
        case AppHealth.SetupStatus.Unhealthy(alert) =>
          buildAlertHttpRequest[F, SetupAlert](config, appInfo, alert)
      }
      .evalMap(executeHttpRequest(config, httpClient, _))
      .drain

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  private def buildAlertHttpRequest[F[_]: Sync, SetupAlert: Show](
    webhookConfig: Config,
    appInfo: AppInfo,
    reason: SetupAlert
  ): Stream[F, Request[F]] =
    webhookConfig.endpoint match {
      case Some(endpoint) =>
        val request = Request[F](Method.POST, endpoint)
          .withEntity(toAlertSdj(reason, appInfo, webhookConfig.tags))
        Stream.emit(request)
      case None =>
        Stream.eval(Logger[F].info(show"Skipping setup alert because webhook is not configured: $reason")).drain
    }

  private def buildHeartbeatHttpRequest[F[_]: Sync](
    webhookConfig: Config,
    appInfo: AppInfo
  ): Stream[F, Request[F]] =
    webhookConfig.endpoint match {
      case Some(endpoint) =>
        val request = Request[F](Method.POST, endpoint)
          .withEntity(toHeartbeatSdj(appInfo, webhookConfig.tags))
        Stream.emit(request)
      case None =>
        Stream.eval(Logger[F].info(s"Skipping heartbeat because webhook is not configured")).drain
    }

  private def executeHttpRequest[F[_]: Async](
    webhookConfig: Config,
    httpClient: Client[F],
    request: Request[F]
  ): F[Unit] =
    httpClient
      .run(request)
      .use { response =>
        if (response.status.isSuccess) Sync[F].unit
        else {
          response
            .as[String]
            .flatMap(body => Logger[F].error(show"Webhook ${webhookConfig.endpoint} returned non-2xx response:\n$body"))
        }
      }
      .handleErrorWith { e =>
        Logger[F].error(e)(show"Webhook ${webhookConfig.endpoint} resulted in exception without a response")
      }

  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

  private def toAlertSdj[SetupAlert: Show](
    reason: SetupAlert,
    appInfo: AppInfo,
    tags: Map[String, String]
  ): Json =
    SelfDescribingData(
      schema = SchemaKey("com.snowplowanalytics.monitoring.loader", "alert", "jsonschema", SchemaVer.Full(1, 0, 0)),
      data = Json.obj(
        "appName" -> appInfo.name.asJson,
        "appVersion" -> appInfo.version.asJson,
        "message" -> reason.show.take(MaxAlertPayloadLength).asJson,
        "tags" -> tags.asJson
      )
    ).normalize

  private def toHeartbeatSdj(
    appInfo: AppInfo,
    tags: Map[String, String]
  ): Json =
    SelfDescribingData(
      schema = SchemaKey("com.snowplowanalytics.monitoring.loader", "heartbeat", "jsonschema", SchemaVer.Full(1, 0, 0)),
      data = Json.obj(
        "appName" -> appInfo.name.asJson,
        "appVersion" -> appInfo.version.asJson,
        "tags" -> tags.asJson
      )
    ).normalize

  def repeatPeriodically[F[_]: Async, A](period: FiniteDuration): Pipe[F, A, A] = {

    def go(timedPull: Pull.Timed[F, A], toRepeat: Option[A]): Pull[F, A, Unit] =
      timedPull.uncons.flatMap {
        case None =>
          // Upstream finished
          Pull.done
        case Some((Left(_), next)) =>
          // Timer timed-out.
          Pull.outputOption1[F, A](toRepeat) *> timedPull.timeout(period) *> go(next, toRepeat)
        case Some((Right(chunk), next)) =>
          chunk.last match {
            case Some(last) =>
              Pull.output[F, A](chunk) *> timedPull.timeout(period) *> go(next, Some(last))
            case None =>
              go(next, toRepeat)
          }
      }

    in =>
      in.pull.timed { timedPull =>
        go(timedPull, None)
      }.stream
  }

}
