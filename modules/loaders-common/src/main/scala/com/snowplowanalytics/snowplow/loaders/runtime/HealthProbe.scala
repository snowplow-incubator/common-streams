/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.runtime

import cats.effect.{Async, Resource, Sync}
import cats.data.Kleisli
import cats.implicits._
import com.comcast.ip4s.{Ipv4Address, Port}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.{HttpApp, Response, Status}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object HealthProbe {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  sealed trait Status
  case object Healthy extends Status
  case class Unhealthy(reason: String) extends Status

  def resource[F[_]: Async](port: Port, isHealthy: F[Status]): Resource[F, Unit] =
    EmberServerBuilder
      .default[F]
      .withHost(Ipv4Address.fromBytes(0, 0, 0, 0))
      .withPort(port)
      .withMaxConnections(1)
      .withHttpApp(httpApp(isHealthy))
      .build
      .evalTap { _ =>
        Logger[F].info(s"Health service listening on port $port")
      }
      .void

  private def httpApp[F[_]: Sync](isHealthy: F[Status]): HttpApp[F] =
    Kleisli { _ =>
      isHealthy.flatMap {
        case Healthy =>
          Logger[F].debug("Health probe returning 200").as {
            Response(status = Status.Ok)
          }
        case Unhealthy(reason) =>
          Logger[F].warn(s"Health probe returning 503: $reason").as {
            Response(status = Status.ServiceUnavailable)
          }
      }
    }

}
