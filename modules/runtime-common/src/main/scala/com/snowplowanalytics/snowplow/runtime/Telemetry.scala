/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.data.NonEmptyList
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.Random
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.scalatracker.Emitter.{Result => TrackerResult, _}
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s._
import fs2.Stream
import io.circe._
import io.circe.syntax._
import io.circe.config.syntax._
import io.circe.generic.semiauto._
import org.http4s.client.{Client => Http4sClient}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.FiniteDuration

import java.util.UUID

object Telemetry {

  case class Config(
    disable: Boolean,
    interval: FiniteDuration,
    collectorUri: String,
    collectorPort: Int,
    secure: Boolean,
    userProvidedId: Option[String],
    autoGeneratedId: Option[String],
    instanceId: Option[String],
    moduleName: Option[String],
    moduleVersion: Option[String]
  )

  object Config {
    implicit def telemetryConfigDecoder: Decoder[Config] = deriveDecoder
  }

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](
    config: Config,
    appInfo: AppInfo,
    httpClient: Http4sClient[F]
  ): Stream[F, Nothing] =
    if (config.disable)
      Stream.never
    else {
      val stream = for {
        uuid <- Stream.eval(Async[F].delay(UUID.randomUUID))
        sdj = makeHeartbeatEvent(config, appInfo, uuid)
        tracker <- Stream.resource(initTracker(config, appInfo.name, httpClient))
        _ <- Stream.unit ++ Stream.fixedDelay[F](config.interval)
        _ <- Stream.eval(tracker.trackSelfDescribingEvent(unstructEvent = sdj))
        _ <- Stream.eval(tracker.flushEmitters())
      } yield ()

      stream.drain
    }

  private def initTracker[F[_]: Async](
    config: Config,
    appName: String,
    client: Http4sClient[F]
  ): Resource[F, Tracker[F]] =
    for {
      implicit0(random: Random[F]) <- Resource.eval(Random.scalaUtilRandom)
      emitter <- Http4sEmitter.build(
                   EndpointParams(config.collectorUri, port = Some(config.collectorPort), https = config.secure),
                   client,
                   retryPolicy = RetryPolicy.MaxAttempts(10),
                   callback    = Some(emitterCallback[F] _)
                 )
    } yield new Tracker(NonEmptyList.of(emitter), "telemetry", appName)

  private def emitterCallback[F[_]: Sync](
    params: EndpointParams,
    req: Request,
    res: TrackerResult
  ): F[Unit] =
    res match {
      case TrackerResult.Success(_) =>
        Logger[F].debug(s"Telemetry heartbeat successfully sent to ${params.getGetUri}")
      case TrackerResult.Failure(code) =>
        Logger[F].warn(s"Sending telemetry heartbeat got unexpected HTTP code $code from ${params.getUri}")
      case TrackerResult.TrackerFailure(exception) =>
        Logger[F].warn(exception)(
          s"Telemetry heartbeat failed to reach ${params.getUri} after ${req.attempt} attempts"
        )
      case TrackerResult.RetriesExceeded(failure) =>
        Logger[F].warn(s"Stopped trying to send telemetry heartbeat after failure: $failure")
    }

  private def makeHeartbeatEvent(
    config: Config,
    appInfo: AppInfo,
    appGeneratedId: UUID
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.oss", "oss_context", "jsonschema", SchemaVer.Full(1, 0, 1)),
      Json.obj(
        "userProvidedId" -> config.userProvidedId.asJson,
        "autoGeneratedId" -> config.autoGeneratedId.asJson,
        "moduleName" -> config.moduleName.asJson,
        "moduleVersion" -> config.moduleVersion.asJson,
        "instanceId" -> config.instanceId.asJson,
        "appGeneratedId" -> appGeneratedId.toString.asJson,
        "cloud" -> appInfo.cloud.asJson,
        "region" -> Json.Null,
        "applicationName" -> appInfo.name.asJson,
        "applicationVersion" -> appInfo.version.asJson
      )
    )

}
