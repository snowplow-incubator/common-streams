/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.{Async, Ref, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import fs2.Stream
import io.circe.Decoder
import io.circe.config.syntax._
import io.circe.generic.semiauto._
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.micrometer.core.instrument.{Counter, Timer}
import io.micrometer.core.instrument.binder.jvm.{JvmGcMetrics, JvmHeapPressureMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.prometheusmetrics.{PrometheusConfig => MicrometerPrometheusConfig, PrometheusMeterRegistry}

import java.time.{Duration => JavaDuration}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicLong

object Metrics {

  /**
   * Provides dual-protocol metrics: prometheus (via micrometer) and statsd.
   *
   * Each registered entry maintains separate internal state for the two protocols. Prometheus
   * metrics are accumulated continuously by micrometer and scraped on demand via the /metrics
   * endpoint. Statsd metrics are snapshot-and-reset: accumulated in Refs, reported periodically,
   * then zeroed.
   */
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

  case class PrometheusConfig(
    tags: Map[String, String]
  )

  object PrometheusConfig {
    implicit val prometheusConfigDecoder: Decoder[PrometheusConfig] =
      deriveDecoder[PrometheusConfig]
  }

  trait CounterEntry[F[_]] {
    def add(count: Long): F[Unit]
  }

  trait GaugeEntry[F[_]] {
    def set(value: Long): F[Unit]
  }

  trait TimerEntry[F[_]] {
    def record(duration: FiniteDuration): F[Unit]
  }

  trait Entries[F[_]] {
    def counter(name: String): F[CounterEntry[F]]
    def gauge(name: String): F[GaugeEntry[F]]
    def timer(name: String, alternativeMaximum: F[Option[FiniteDuration]]): F[TimerEntry[F]]
    def scrape: F[String]
    def report: Stream[F, Nothing]
  }

  def build[F[_]: Async](
    statsdConfig: Option[StatsdConfig],
    prometheusConfig: PrometheusConfig
  ): Resource[F, Entries[F]] =
    for {
      registry <- Resource.make(Sync[F].delay {
                    val r = new PrometheusMeterRegistry(MicrometerPrometheusConfig.DEFAULT)
                    prometheusConfig.tags.foreach { case (k, v) =>
                      r.config().commonTags(k, v)
                    }
                    r
                  })(r => Sync[F].delay(r.close()))
      _ <- Resource.eval(Sync[F].delay(new JvmMemoryMetrics().bindTo(registry)))
      _ <- Resource.fromAutoCloseable(Sync[F].delay(new JvmGcMetrics())).evalMap(m => Sync[F].delay(m.bindTo(registry)))
      _ <- Resource.fromAutoCloseable(Sync[F].delay(new JvmHeapPressureMetrics())).evalMap(m => Sync[F].delay(m.bindTo(registry)))
      _ <- Resource.eval(Sync[F].delay(new JvmThreadMetrics().bindTo(registry)))
      registeredEntries <- Resource.eval(Ref[F].of(List.empty[InternalEntry[F]]))
    } yield new Entries[F] {
      def counter(name: String): F[CounterEntry[F]] =
        for {
          accumulator <- Ref[F].of(0L)
          micrometerCounter <- Sync[F].delay(registry.counter(name))
          entry = new InternalCounterEntry[F](name, accumulator, micrometerCounter)
          _ <- registeredEntries.update(entry :: _)
        } yield entry

      def gauge(name: String): F[GaugeEntry[F]] =
        for {
          statsdMax <- Ref[F].of(0L)
          backing <- Sync[F].delay(new AtomicLong(0L))
          _ <- Sync[F].delay(registry.gauge(name, backing))
          entry = new InternalGaugeEntry[F](name, statsdMax, backing)
          _ <- registeredEntries.update(entry :: _)
        } yield entry

      def timer(name: String, alternativeMaximum: F[Option[FiniteDuration]]): F[TimerEntry[F]] =
        for {
          statsdMax <- Ref[F].of(Duration.Zero)
          micrometerTimer <- Sync[F].delay {
                               Timer
                                 .builder(name)
                                 .publishPercentileHistogram(false)
                                 .distributionStatisticExpiry(JavaDuration.ofSeconds(60))
                                 .register(registry)
                             }
          entry = new InternalTimerEntry[F](name, statsdMax, alternativeMaximum, micrometerTimer)
          _ <- registeredEntries.update(entry :: _)
        } yield entry

      def scrape: F[String] =
        Sync[F].delay(registry.scrape())

      def report: Stream[F, Nothing] = {
        def doReport(reporters: List[Reporter[F]], allEntries: List[InternalEntry[F]]): F[Unit] =
          for {
            kvs <- allEntries.traverse(_.snapshotAndReset)
            _ <- reporters.traverse(_.report(kvs))
          } yield ()

        val stream = for {
          reporters <- Stream.resource(makeReporters[F](statsdConfig))
          allEntries <- Stream.eval(registeredEntries.get)
          _ <- Stream
                 .fixedDelay[F](statsdConfig.fold(1.minute)(_.period))
                 .evalMap(_ => doReport(reporters, allEntries))
                 .onFinalize(doReport(reporters, allEntries))
        } yield ()

        stream.drain
      }
    }

  /** Private implementation */

  private sealed trait InternalEntry[F[_]] {
    def name: String
    def snapshotAndReset: F[KVMetric]
  }

  private class InternalCounterEntry[F[_]](
    val name: String,
    accumulator: Ref[F, Long],
    micrometerCounter: Counter
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with CounterEntry[F] {

    def add(count: Long): F[Unit] =
      accumulator.update(_ + count) *>
        F.delay(micrometerCounter.increment(count.toDouble))

    def snapshotAndReset: F[KVMetric] =
      accumulator.getAndSet(0L).map { value =>
        KVMetric(name, value.toString, MetricType.Count)
      }
  }

  /**
   * @param statsdMax
   *   Tracks the peak value within each statsd reporting period
   * @param backing
   *   AtomicLong because micrometer's gauge API polls a java.lang.Number
   */
  private class InternalGaugeEntry[F[_]](
    val name: String,
    statsdMax: Ref[F, Long],
    backing: AtomicLong
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with GaugeEntry[F] {

    def set(value: Long): F[Unit] =
      statsdMax.update(current => math.max(current, value)) *>
        F.delay(backing.set(value))

    def snapshotAndReset: F[KVMetric] =
      statsdMax.getAndSet(0L).map { value =>
        KVMetric(name, value.toString, MetricType.Gauge)
      }
  }

  /**
   * @param statsdMax
   *   Tracks the peak duration within each statsd reporting period
   * @param alternativeMaximum
   *   An external source of "true" maximum (e.g. stream latency from the source) which may exceed
   *   what was recorded via `record()`. Compared at snapshot time and the larger value is reported.
   */
  private class InternalTimerEntry[F[_]](
    val name: String,
    statsdMax: Ref[F, FiniteDuration],
    alternativeMaximum: F[Option[FiniteDuration]],
    micrometerTimer: Timer
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with TimerEntry[F] {

    def record(duration: FiniteDuration): F[Unit] =
      statsdMax.update(current => current.max(duration)) *>
        F.delay(micrometerTimer.record(duration.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))

    def snapshotAndReset: F[KVMetric] =
      for {
        recorded <- statsdMax.getAndSet(Duration.Zero)
        alternative <- alternativeMaximum
        value = alternative.fold(recorded)(recorded.max(_))
      } yield KVMetric(name, value.toMillis.toString, MetricType.Gauge)
  }

  private case class KVMetric(
    key: String,
    value: String,
    metricType: MetricType
  )

  private sealed trait MetricType {
    def render: Char
  }

  private object MetricType {
    case object Gauge extends MetricType { def render = 'g' }
    case object Count extends MetricType { def render = 'c' }
  }

  /**
   * The raw config received by combining user-provided config with snowplow defaults
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
