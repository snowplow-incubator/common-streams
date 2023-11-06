/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Resource}
import cats.implicits._
import fs2.Stream
import io.circe.Decoder
import io.circe.config.syntax._
import io.circe.generic.semiauto._
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

abstract class Metrics[F[_]: Async, S <: Metrics.State](
  ref: Ref[F, S],
  emptyState: S,
  config: Option[Metrics.StatsdConfig]
) {
  def report: Stream[F, Nothing] =
    Stream.resource(Metrics.makeReporters[F](config)).flatMap { reporters =>
      def report = for {
        state <- ref.getAndSet(emptyState)
        kv = state.toKVMetrics
        _ <- reporters.traverse(_.report(kv))
      } yield ()

      val stream = for {
        _ <- Stream.fixedDelay[F](config.fold(1.minute)(_.period))
        _ <- Stream.eval(report)
      } yield ()

      stream.drain.onFinalize(report)
    }
}

object Metrics {

  /** Public API */

  case class StatsdConfig(
    hostname: String,
    port: Int,
    tags: Map[String, String],
    period: FiniteDuration,
    prefix: String
  )

  object StatsdConfig {
    implicit def stasdConfigDecoder: Decoder[Option[StatsdConfig]] =
      deriveDecoder[StatsdUnresolvedConfig].map(resolveConfig(_))
  }

  trait State {
    def toKVMetrics: List[KVMetric]
  }

  sealed trait MetricType {
    def render: Char
  }

  object MetricType {
    case object Gauge extends MetricType { def render = 'g' }
    case object Count extends MetricType { def render = 'c' }
  }

  trait KVMetric {
    def key: String
    def value: String
    def metricType: MetricType
  }

  /** Private implementation */

  /**
   * The raw config received by combinging user-provided config with snowplow defaults
   *
   * If user did not configure statsd, then hostname is None and all other params are defined via
   * our defaults.
   */
  private case class StatsdUnresolvedConfig(
    hostname: Option[String],
    port: Int,
    tags: Map[String, String],
    period: FiniteDuration,
    prefix: String
  )

  private def resolveConfig(from: StatsdUnresolvedConfig): Option[StatsdConfig] =
    from match {
      case StatsdUnresolvedConfig(Some(hostname), port, tags, period, prefix) =>
        Some(StatsdConfig(hostname, port, tags, period, prefix))
      case StatsdUnresolvedConfig(None, _, _, _, _) =>
        None
    }

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private trait Reporter[F[_]] {
    def report(metrics: List[KVMetric]): F[Unit]
  }

  private def stdoutReporter[F[_]: Sync]: Reporter[F] = new Reporter[F] {
    def report(metrics: List[KVMetric]): F[Unit] =
      metrics.traverse_ { kv =>
        Logger[F].info(s"${kv.key} = ${kv.value}")
      }
  }

  private def makeReporters[F[_]: Sync](config: Option[StatsdConfig]): Resource[F, List[Reporter[F]]] =
    config match {
      case None => Resource.pure(List(stdoutReporter[F]))
      case Some(c) =>
        Resource
          .fromAutoCloseable(Sync[F].delay(new DatagramSocket))
          .map { socket =>
            List(stdoutReporter, statsdReporter(c, socket))
          }
    }

  private def statsdReporter[F[_]: Sync](config: StatsdConfig, socket: DatagramSocket): Reporter[F] = new Reporter[F] {

    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix.stripSuffix(".")

    def report(metrics: List[KVMetric]): F[Unit] =
      Sync[F]
        .blocking(InetAddress.getByName(config.hostname))
        .flatMap { addr =>
          Sync[F].blocking {
            metrics.foreach { kv =>
              val str    = s"${prefix}.${kv.key}:${kv.value}|${kv.metricType.render}|#$tagStr".stripPrefix(".")
              val bytes  = str.getBytes(UTF_8)
              val packet = new DatagramPacket(bytes, bytes.length, addr, config.port)
              socket.send(packet)
            }
          }
        }
        .handleErrorWith { t =>
          Logger[F].warn(t)("Caught exception sending statsd metrics")
        }
  }

}
