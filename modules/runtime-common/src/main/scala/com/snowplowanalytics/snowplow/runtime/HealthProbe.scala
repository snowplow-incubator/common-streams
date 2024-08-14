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
import cats.data.Kleisli
import cats.effect.{Async, Resource, Sync}
import com.comcast.ip4s.{Ipv4Address, Port}
import io.circe.Decoder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.{HttpApp, Response, Status}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import fs2.io.net.Network

object HealthProbe {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async, RuntimeService: Show](
    port: Port,
    appHealth: AppHealth[F, ?, RuntimeService]
  ): Resource[F, Unit] = {
    implicit val network: Network[F] = Network.forAsync[F]
    EmberServerBuilder
      .default[F]
      .withHost(Ipv4Address.fromBytes(0, 0, 0, 0))
      .withPort(port)
      .withMaxConnections(1)
      .withHttpApp(httpApp(appHealth))
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

  private[runtime] def httpApp[F[_]: Sync, RuntimeService: Show](
    appHealth: AppHealth[F, ?, RuntimeService]
  ): HttpApp[F] =
    Kleisli { _ =>
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
          val joined = allUnhealthy.mkString("Services are awaiting a healthy status [", ", ", "]")
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
