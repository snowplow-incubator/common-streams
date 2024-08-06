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

import cats.effect.{Async, Sync}
import cats.implicits._
import cats.Show
import io.circe.{Decoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s.circe.jsonEncoder
import org.http4s.client.Client
import org.http4s.{Method, Request}
import org.http4s.{ParseFailure, Uri}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.runtime.AppInfo

trait Webhook[F[_], Alert] {
  def alert(message: Alert): F[Unit]
}

object Webhook {

  final case class Config(endpoint: Uri, tags: Map[String, String])

  object Config {
    implicit def webhookConfigDecoder: Decoder[Config] = {
      implicit val http4sUriDecoder: Decoder[Uri] =
        Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))
      deriveDecoder[Config]
    }
  }

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def create[F[_]: Async, Alert: Show](
    config: Option[Config],
    appInfo: AppInfo,
    httpClient: Client[F]
  ): Webhook[F, Alert] = new Webhook[F, Alert] {

    override def alert(message: Alert): F[Unit] =
      config match {
        case Some(webhookConfig) =>
          val request = buildHttpRequest[F, Alert](webhookConfig, appInfo, message)
          Logger[F].info(show"Sending alert to ${webhookConfig.endpoint} with details of the setup error...") *>
            executeHttpRequest[F](webhookConfig, httpClient, request)
        case None =>
          Logger[F].debug(s"Webhook monitoring is not configured, skipping alert: $message")
      }
  }

  private def buildHttpRequest[F[_], Alert: Show](
    webhookConfig: Config,
    appInfo: AppInfo,
    alert: Alert
  ): Request[F] =
    Request[F](Method.POST, webhookConfig.endpoint)
      .withEntity(toSelfDescribingJson(alert, appInfo, webhookConfig.tags))

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

  private def toSelfDescribingJson[Alert: Show](
    alert: Alert,
    appInfo: AppInfo,
    tags: Map[String, String]
  ): Json =
    SelfDescribingData(
      schema = SchemaKey("com.snowplowanalytics.monitoring.loader", "alert", "jsonschema", SchemaVer.Full(1, 0, 0)),
      data = Json.obj(
        "appName" -> appInfo.name.asJson,
        "appVersion" -> appInfo.version.asJson,
        "message" -> alert.show.take(MaxAlertPayloadLength).asJson,
        "tags" -> tags.asJson
      )
    ).normalize

}
