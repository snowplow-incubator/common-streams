/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import java.io._
import java.net._
import scala.jdk.CollectionConverters._
import cats.implicits._
import cats.effect.{Resource, Sync}
import io.circe.parser._

trait StatsdAPI[F[_]] {
  def get(metricType: Metrics.MetricType): F[Map[String, Int]]
}

object StatsdAPI {
  def resource[F[_]: Sync](socket: Socket): Resource[F, StatsdAPI[F]] = for {
    output <- Resource.eval(Sync[F].pure(new PrintWriter(socket.getOutputStream(), true)))
    input <- Resource.eval(Sync[F].pure(new BufferedReader(new InputStreamReader(socket.getInputStream()))))
  } yield new StatsdAPI[F] {
    def get(metricType: Metrics.MetricType): F[Map[String, Int]] = for {
      _ <- Sync[F].blocking(output.println(showMetric(metricType)))
      json <-
        Sync[F].pure(input.lines().iterator().asScala.takeWhile(!_.toLowerCase().contains("end")).mkString("\n").replaceAll("'", "\""))
      res <- Sync[F].fromEither(decode[Map[String, Int]](json))
      _ <- Sync[F].pure(println(s"""StatsD metrics received: ${res.mkString(", ")}"""))
    } yield res

    private[this] def showMetric(metricType: Metrics.MetricType) = metricType match {
      case Metrics.MetricType.Count => "counters"
      case Metrics.MetricType.Gauge => "gauges"
    }
  }
}
