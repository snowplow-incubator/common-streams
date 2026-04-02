/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.Show
import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import com.comcast.ip4s.{Ipv4Address, Port}
import io.circe.Decoder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.{HttpApp, HttpRoutes, Response, Status}
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import fs2.io.net.Network

object HealthProbe {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async, RuntimeService: Show](
    port: Port,
    appHealth: AppHealth[F, ?, RuntimeService],
    scrapeMetrics: F[String]
  ): Resource[F, Unit] = {
    implicit val network: Network[F] = Network.forAsync[F]
    EmberServerBuilder
      .default[F]
      .withHost(Ipv4Address.fromBytes(0, 0, 0, 0))
      .withPort(port)
      .withMaxConnections(1)
      .withHttpApp(httpApp(appHealth, scrapeMetrics))
      .build
      .evalTap { _ =>
        Logger[F].info(s"Health service listening on port $port")
      }
      .void
  }

  object decoders {
    implicit def portDecoder: Decoder[Port] = Decoder.decodeInt.emap { port =>
      Port.fromInt(port).toRight("Invalid port")
    }
  }

  /**
   * Routes GET /metrics to the prometheus scrape endpoint; all other requests return the health
   * check status.
   */
  private[runtime] def httpApp[F[_]: Sync, RuntimeService: Show](
    appHealth: AppHealth[F, ?, RuntimeService],
    scrapeMetrics: F[String]
  ): HttpApp[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    HttpRoutes
      .of[F] {
        case GET -> Root / "metrics" =>
          scrapeMetrics.map { body =>
            Response[F](status = Status.Ok)
              .withEntity(body)
              .withContentType(`Content-Type`(MediaType.text.plain))
          }
        case _ =>
          healthResponse(appHealth)
      }
      .orNotFound
  }

  private def healthResponse[F[_]: Sync, RuntimeService: Show](
    appHealth: AppHealth[F, ?, RuntimeService]
  ): F[Response[F]] = {
    val problemsF = for {
      runtimeUnhealthies <- appHealth.unhealthyRuntimeServiceMessages
      setupHealth <- appHealth.setupHealth.get
    } yield {
      val allUnhealthy = runtimeUnhealthies ++ (setupHealth match {
        case AppHealth.SetupStatus.Unhealthy(_) => Some("External setup configuration")
        case _                                  => None
      })

      val allAwaiting = setupHealth match {
        case AppHealth.SetupStatus.AwaitingHealth => Some("External setup configuration")
        case _                                    => None
      }

      val unhealthyMsg = if (allUnhealthy.nonEmpty) {
        val joined = allUnhealthy.mkString("Services are unhealthy [", ", ", "]")
        Some(joined)
      } else None

      val awaitingMsg = if (allAwaiting.nonEmpty) {
        val joined = allAwaiting.mkString("Services are awaiting a healthy status [", ", ", "]")
        Some(joined)
      } else None

      if (unhealthyMsg.isEmpty && awaitingMsg.isEmpty)
        None
      else
        Some((unhealthyMsg ++ awaitingMsg).mkString(" AND "))
    }

    problemsF.flatMap {
      case Some(errorMsg) =>
        Logger[F].warn(s"Health probe returning 503: $errorMsg").as {
          Response(status = Status.ServiceUnavailable)
        }
      case None =>
        Logger[F].debug("Health probe returning 200").as {
          Response(status = Status.Ok)
        }
    }
  }

}
