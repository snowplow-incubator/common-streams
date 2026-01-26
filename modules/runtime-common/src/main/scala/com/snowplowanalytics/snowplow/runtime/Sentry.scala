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

import cats.Id
import cats.implicits._
import cats.effect.{Resource, Sync}

import io.circe.Decoder
import io.circe.generic.semiauto._

import io.sentry.{Sentry => SentrySdk}

object Sentry {

  case class ConfigM[M[_]](
    dsn: M[String],
    environment: Option[String],
    tags: Map[String, String]
  )

  object ConfigM {
    implicit val sentryDecoder: Decoder[Option[Config]] = deriveDecoder[ConfigM[Option]]
      .map[Option[Config]] {
        case ConfigM(Some(dsn), environment, tags) =>
          Some(ConfigM[Id](dsn, environment, tags))
        case ConfigM(None, _, _) =>
          None
      }
  }

  type Config = ConfigM[Id]

  def enable[F[_]: Sync](appInfo: AppInfo, config: Option[Config]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          SentrySdk.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.environment.foreach(options.setEnvironment)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(SentrySdk.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

}
