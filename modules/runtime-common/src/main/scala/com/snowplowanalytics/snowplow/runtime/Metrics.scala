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

import io.micrometer.core.instrument.{Counter, Gauge, Timer}
import io.micrometer.core.instrument.binder.jvm.{JvmGcMetrics, JvmHeapPressureMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.prometheusmetrics.{PrometheusConfig => MicrometerPrometheusConfig, PrometheusMeterRegistry}

import java.time.{Duration => JavaDuration}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicLong

object Metrics {

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

  /**
   * Where a registered metric is sent.
   *
   * Prometheus metrics are cheap for us to collect, whereas statsd metrics are comparatively
   * expensive. There is deliberately no default: the cost of a metric is a decision to be made
   * explicitly at the point of registration.
   */
  sealed trait Destination

  object Destination {

    /**
     * Accumulated by micrometer and exposed via the /metrics endpoint only.
     *
     * This also excludes the metric from the periodic stdout report, which is written even when
     * statsd is not configured. So it is not a no-op for an app with no statsd config: the metric
     * stops appearing in the logs too.
     */
    case object PrometheusOnly extends Destination

    /** Additionally snapshotted and sent to statsd on each reporting cycle */
    case object PrometheusAndStatsd extends Destination
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

  /**
   * Records observations of how far behind the application is; returned by [[Entries.lagGauge]].
   */
  trait LagEntry[F[_]] {
    def record(duration: FiniteDuration): F[Unit]
  }

  trait Entries[F[_]] {
    def counter(name: String, destination: Destination): F[CounterEntry[F]]
    def gauge(name: String, destination: Destination): F[GaugeEntry[F]]

    /**
     * A metric of how far behind this application is, e.g. the age of the oldest message it has not
     * yet finished with.
     *
     * The prometheus value is the larger of two terms, and only one of them decays. What decays is
     * the remembered peak of batches which have already completed: that is held for a couple of
     * minutes and then expires. What does not decay is `currentLag`, which is recomputed as `now -
     * tstamp` from the in-flight batch on every read rather than stored and left to age. So while
     * an output is stuck - a warehouse outage, say - the published gauge grows without bound for as
     * long as the outage lasts, and never falls back to zero.
     *
     * It does fall back to zero when the source stalls instead - a lost Kinesis lease, a wedged
     * pull - because then there is no batch in flight to give a `currentLag`, and the remembered
     * peaks expire. That is by design, and the two protocols carry the stalled-source signal by
     * opposite means, so an alert migrated from one to the other is equivalent rather than a
     * downgrade. On statsd the signal is absence: a stalled source records nothing and holds no
     * batch in flight, so `snapshotAndReset` combines two Nones and emits no metric at all - the
     * series goes quiet and the missing-metrics alert fires. Healthy-idle stays distinguishable
     * from that, because a successful empty poll still records a zero, which is emitted. Prometheus
     * cannot express absence, since a registered gauge is always present and reads 0 in both cases,
     * so there the signal moves into a second series: `rate(<name>_observations_total[5m]) == 0` is
     * flat only for the stalled source, because healthy-idle keeps advancing that counter. The
     * counter is prometheus-only deliberately; adding a statsd series would change statsd's output.
     *
     * Both signals have the same exception: for Kafka, NSQ and Http a stalled source goes on
     * recording zeros, so statsd never falls silent and that counter never goes flat. Do not build
     * a stalled-source alert on either expression for those three; `LowLevelSource.stream` in
     * streams-core has the mechanism.
     *
     * The stuck-output guarantee has its own second exception, for a stream which does not record a
     * source timestamp: `currentLag` is then always None and every observation is zero, so once the
     * window expires a stuck output reads a healthy zero here and goes flat on the observations
     * counter instead. Something still alerts, but it says "stalled source" when the source is
     * fine; `LowLevelSource.InternalState.AwaitingDownstream` in streams-core says when the
     * timestamp is absent.
     *
     * The published value is floored at zero on both protocols; see `DecayingMax.record` for why a
     * negative is reachable at all.
     *
     * A name must be registered only once. Micrometer returns the meter already registered under a
     * matching id and discards the new configuration, so a second registration of the same name
     * yields a second entry whose gauge was never registered: it joins the refresh list, is
     * refreshed on every scrape, and publishes nothing.
     *
     * @param currentLag
     *   An external source of "true" lag which may exceed anything passed to `record`, e.g. a batch
     *   which is still being retried downstream. It is re-evaluated on every statsd report and on
     *   every prometheus scrape, so it can grow while nothing is being recorded. It must not raise;
     *   neither read path guards against it.
     */
    def lagGauge(
      name: String,
      currentLag: F[Option[FiniteDuration]],
      destination: Destination
    ): F[LagEntry[F]]

    /**
     * A metric of the distribution of a duration, e.g. end-to-end latency of loaded batches.
     *
     * The unit of observation is one batch, timestamped by its earliest event, so a quantile reads
     * "99% of batches completed within X of their oldest event" - batches, not events.
     *
     * A timer never consults an external lag source; it reports only what was passed to `record`.
     * If you need one, use [[lagGauge]].
     *
     * The histogram's bucket range is derived from `expectedLatency`: a tenth of it below, a
     * hundred times it above, snapped to micrometer's standard boundaries. The range is skewed
     * upward because latency degrades upward and essentially never downward. Everything above the
     * ceiling falls into `+Inf`, so a hint set too low makes `histogram_quantile` saturate there
     * and read back as a flat, plausible-looking value; the timer's own `_max` gauge is not
     * bucketed, so it escapes the ceiling, but it is a rolling maximum over the last 60 seconds
     * rather than the true peak.
     *
     * `expectedLatency` has deliberately no default, even though a wrong hint is a milder mistake
     * than a wrong name: a unit-suffixed name would rename a live statsd series, so it is rejected
     * outright, whereas a bad range only degrades into a saturated quantile. The reason to require
     * it anyway is that only the application knows where its latency sits, so any default we chose
     * would be silently wrong for most callers.
     *
     * A name must be registered only once. Micrometer returns the meter already registered under a
     * matching id and discards the new configuration, so a second registration of the same name
     * yields a second entry writing into the first one's timer, with the first one's bucket range
     * and not the range its own `expectedLatency` asks for.
     *
     * @param expectedLatency
     *   Roughly where this metric's values sit in normal operation. Must be positive; rejected at
     *   registration otherwise.
     */
    def timer(
      name: String,
      expectedLatency: FiniteDuration,
      destination: Destination
    ): F[TimerEntry[F]]
    def scrape: F[String]
    def report: Stream[F, Nothing]
  }

  /**
   * Provides dual-protocol metrics: prometheus (via micrometer) and statsd.
   *
   * Every metric is registered with an explicit [[Destination]]. A `PrometheusAndStatsd` entry
   * maintains separate internal state for the two protocols; a `PrometheusOnly` entry carries no
   * statsd bookkeeping at all.
   *
   * Prometheus metrics are accumulated continuously by micrometer and scraped on demand via the
   * /metrics endpoint. Statsd metrics are snapshot-and-reset: accumulated in Refs, reported
   * periodically, then zeroed. Counters are reported on every cycle (emitting 0 when quiet, so
   * dashboards see a continuous time-series instead of gaps); gauges and timers are suppressed when
   * they have not been touched since the last report.
   */
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
      refreshables <- Resource.eval(Ref[F].of(List.empty[PrometheusRefresh[F]]))
      reporters <- makeReporters[F](statsdConfig)
    } yield createEntries(registry, registeredEntries, refreshables, reporters, statsdConfig.fold(1.minute)(_.period))

  private[runtime] def createEntries[F[_]: Async](
    registry: PrometheusMeterRegistry,
    registeredEntries: Ref[F, List[InternalEntry[F]]],
    refreshables: Ref[F, List[PrometheusRefresh[F]]],
    reporters: List[Reporter[F]],
    metricEmitPeriod: FiniteDuration
  ): Entries[F] =
    new Entries[F] {
      override def counter(name: String, destination: Destination): F[CounterEntry[F]] =
        Sync[F].delay(registry.counter(name)).flatMap { micrometerCounter =>
          destination match {
            case Destination.PrometheusOnly =>
              Sync[F].pure(new PrometheusOnlyCounterEntry[F](micrometerCounter))
            case Destination.PrometheusAndStatsd =>
              for {
                accumulator <- Ref[F].of(0L)
                entry = new PrometheusAndStatsdCounterEntry[F](name, accumulator, micrometerCounter)
                _ <- registeredEntries.update(entry :: _)
              } yield entry
          }
        }

      override def gauge(name: String, destination: Destination): F[GaugeEntry[F]] =
        Sync[F]
          .delay(new AtomicLong(0L))
          .flatTap { backing =>
            Sync[F].delay {
              // A strong reference, because micrometer otherwise holds the backing object weakly
              // and a PrometheusOnly gauge is not kept alive by the list of statsd entries
              Gauge
                .builder(name, backing, (b: AtomicLong) => b.doubleValue())
                .strongReference(true)
                .register(registry)
            }
          }
          .flatMap { backing =>
            destination match {
              case Destination.PrometheusOnly =>
                Sync[F].pure(new PrometheusOnlyGaugeEntry[F](backing))
              case Destination.PrometheusAndStatsd =>
                for {
                  statsdMax <- Ref[F].of[Option[Long]](None)
                  entry = new PrometheusAndStatsdGaugeEntry[F](name, statsdMax, backing)
                  _ <- registeredEntries.update(entry :: _)
                } yield entry
            }
          }

      override def lagGauge(
        name: String,
        currentLag: F[Option[FiniteDuration]],
        destination: Destination
      ): F[LagEntry[F]] =
        validateDurationName[F](name) *> {
          for {
            windowMax <- DecayingMax.build[F](lagDecayWindow, lagDecayBuckets)
            backingNanos <- Sync[F].delay(new AtomicLong(0L))
            _ <- Sync[F].delay {
                   // A strong reference, for the same reason as `gauge` above
                   Gauge
                     .builder(name, backingNanos, (b: AtomicLong) => b.get().toDouble / 1e9d)
                     .baseUnit("seconds")
                     .strongReference(true)
                     .register(registry)
                 }
            // See [[Entries.lagGauge]]
            observations <- Sync[F].delay(registry.counter(name + lagObservationsSuffix))
            entry <- registerLagEntry(name, currentLag, windowMax, backingNanos, observations, destination)
          } yield entry
        }

      /**
       * Both destinations join `refreshables`, because the prometheus half of a lag gauge exists
       * either way.
       */
      private def registerLagEntry(
        name: String,
        currentLag: F[Option[FiniteDuration]],
        windowMax: DecayingMax[F],
        backingNanos: AtomicLong,
        observations: Counter,
        destination: Destination
      ): F[LagEntry[F]] =
        destination match {
          case Destination.PrometheusOnly =>
            val entry = new PrometheusOnlyLagEntry[F](currentLag, windowMax, backingNanos, observations)
            refreshables.update(entry :: _).as(entry)
          case Destination.PrometheusAndStatsd =>
            for {
              statsdMax <- Ref[F].of[Option[FiniteDuration]](None)
              entry = new PrometheusAndStatsdLagEntry[F](name, statsdMax, currentLag, windowMax, backingNanos, observations)
              _ <- registeredEntries.update(entry :: _)
              _ <- refreshables.update(entry :: _)
            } yield entry
        }

      override def timer(
        name: String,
        expectedLatency: FiniteDuration,
        destination: Destination
      ): F[TimerEntry[F]] =
        validateDurationName[F](name) *>
          validateExpectedLatency[F](name, expectedLatency) *>
          Sync[F]
            .delay {
              // Micrometer's default range is 1ms-30s. A windowing loader routinely exceeds 30s,
              // which would put every observation in `+Inf` and pin every quantile at the ceiling.
              // `Entries.timer` documents how the range is derived from the caller's hint.
              //
              // `distributionStatisticExpiry` governs only `_max` and the percentiles: the
              // prometheus registry overrides the expiry for histogram buckets, which accumulate
              // for the life of the process. So `publishPercentileHistogram` and a 60-second expiry
              // sitting next to each other do not mean the buckets rotate.
              Timer
                .builder(name)
                .publishPercentileHistogram(true)
                .minimumExpectedValue(JavaDuration.ofNanos((expectedLatency / 10).toNanos))
                .maximumExpectedValue(JavaDuration.ofNanos((expectedLatency * 100).toNanos))
                .distributionStatisticExpiry(JavaDuration.ofSeconds(60))
                .register(registry)
            }
            .flatMap { micrometerTimer =>
              destination match {
                case Destination.PrometheusOnly =>
                  Sync[F].pure(new PrometheusOnlyTimerEntry[F](micrometerTimer))
                case Destination.PrometheusAndStatsd =>
                  for {
                    statsdMax <- Ref[F].of[Option[FiniteDuration]](None)
                    entry = new PrometheusAndStatsdTimerEntry[F](name, statsdMax, micrometerTimer)
                    _ <- registeredEntries.update(entry :: _)
                  } yield entry
              }
            }

      override def scrape: F[String] =
        for {
          toRefresh <- refreshables.get
          _ <- toRefresh.traverse_(_.refreshPrometheus)
          scraped <- Sync[F].delay(registry.scrape())
        } yield scraped

      override def report: Stream[F, Nothing] = {
        def doReport(reporters: List[Reporter[F]], allEntries: List[InternalEntry[F]]): F[Unit] =
          for {
            kvs <- allEntries.traverse(_.snapshotAndReset).map(_.flatten)
            _ <- reporters.traverse(_.report(kvs))
          } yield ()

        val stream = for {
          allEntries <- Stream.eval(registeredEntries.get)
          _ <- Stream
                 .fixedDelay[F](metricEmitPeriod)
                 .evalMap(_ => doReport(reporters, allEntries))
                 .onFinalize(doReport(reporters, allEntries))
        } yield ()

        stream.drain
      }
    }

  /** Private implementation */

  private[runtime] sealed trait InternalEntry[F[_]] {
    def snapshotAndReset: F[Option[KVMetric]]
  }

  /**
   * An entry whose prometheus value is computed at scrape time rather than accumulated by
   * micrometer.
   *
   * Deliberately separate from [[InternalEntry]], which means "has statsd bookkeeping" and which a
   * `PrometheusOnly` entry never joins. A lag gauge needs refreshing whatever its destination.
   */
  private[runtime] sealed trait PrometheusRefresh[F[_]] {

    /**
     * Bring the scrape-time value up to date.
     *
     * Must be idempotent: a prometheus endpoint may be scraped by several clients, retried, or
     * probed.
     */
    def refreshPrometheus: F[Unit]
  }

  private class PrometheusOnlyCounterEntry[F[_]](
    micrometerCounter: Counter
  )(implicit F: Sync[F]
  ) extends CounterEntry[F] {

    override def add(count: Long): F[Unit] =
      F.delay(micrometerCounter.increment(count.toDouble))
  }

  /**
   * @param accumulator
   *   Tracks the count within each statsd reporting period. Unlike gauges/timers, the counter is
   *   reported on every cycle (emitting 0 when quiet) so that dashboards see a continuous
   *   time-series instead of gaps.
   */
  private class PrometheusAndStatsdCounterEntry[F[_]](
    val name: String,
    accumulator: Ref[F, Long],
    micrometerCounter: Counter
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with CounterEntry[F] {

    override def add(count: Long): F[Unit] =
      accumulator.update(_ + count) *>
        F.delay(micrometerCounter.increment(count.toDouble))

    override def snapshotAndReset: F[Option[KVMetric]] =
      accumulator.getAndSet(0L).map { value =>
        KVMetric(name, value.toString, MetricType.Count).some
      }
  }

  /**
   * @param backing
   *   AtomicLong because micrometer's gauge API polls a java.lang.Number
   */
  private class PrometheusOnlyGaugeEntry[F[_]](
    backing: AtomicLong
  )(implicit F: Sync[F]
  ) extends GaugeEntry[F] {

    override def set(value: Long): F[Unit] =
      F.delay(backing.set(value))
  }

  /**
   * @param statsdMax
   *   Tracks the peak value within each statsd reporting period
   * @param backing
   *   AtomicLong because micrometer's gauge API polls a java.lang.Number
   */
  private class PrometheusAndStatsdGaugeEntry[F[_]](
    val name: String,
    statsdMax: Ref[F, Option[Long]],
    backing: AtomicLong
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with GaugeEntry[F] {

    override def set(value: Long): F[Unit] =
      statsdMax.update(current => math.max(current.getOrElse(0L), value).some) *>
        F.delay(backing.set(value))

    override def snapshotAndReset: F[Option[KVMetric]] =
      statsdMax.getAndSet(None).map { value =>
        value.map(v => KVMetric(name, v.toString, MetricType.Gauge))
      }
  }

  private class PrometheusOnlyTimerEntry[F[_]](
    micrometerTimer: Timer
  )(implicit F: Sync[F]
  ) extends TimerEntry[F] {

    override def record(duration: FiniteDuration): F[Unit] =
      F.delay(micrometerTimer.record(duration.toNanos, java.util.concurrent.TimeUnit.NANOSECONDS))
  }

  /**
   * @param statsdMax
   *   Tracks the peak duration within each statsd reporting period
   */
  private class PrometheusAndStatsdTimerEntry[F[_]](
    val name: String,
    statsdMax: Ref[F, Option[FiniteDuration]],
    micrometerTimer: Timer
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with TimerEntry[F] {

    override def record(duration: FiniteDuration): F[Unit] =
      recordStatsdMax(statsdMax, duration) *>
        F.delay(micrometerTimer.record(duration.toNanos, java.util.concurrent.TimeUnit.NANOSECONDS))

    override def snapshotAndReset: F[Option[KVMetric]] =
      statsdMax.getAndSet(None).map { recorded =>
        recorded.map(statsdDurationMetric(name, _))
      }
  }

  /**
   * The statsd bookkeeping shared by [[PrometheusAndStatsdTimerEntry]] and
   * [[PrometheusAndStatsdLagEntry]]: statsd's key and value carry live production alerting, so
   * these two expressions must never drift between the entries.
   */
  private def recordStatsdMax[F[_]](statsdMax: Ref[F, Option[FiniteDuration]], duration: FiniteDuration): F[Unit] =
    statsdMax.update(current => current.getOrElse(Duration.Zero).max(duration).some)

  private def statsdDurationMetric(name: String, value: FiniteDuration): KVMetric =
    KVMetric(name + statsdDurationSuffix, value.toMillis.toString, MetricType.Gauge)

  /**
   * The prometheus value of a lag gauge: the larger of the recent peak and the current lag.
   *
   * Both terms are needed - see [[Entries.lagGauge]] for what each contributes, how the two differ
   * over time, and why `currentLag` is read unguarded.
   */
  private def refreshLagGauge[F[_]: Sync](
    currentLag: F[Option[FiniteDuration]],
    windowMax: DecayingMax[F],
    backingNanos: AtomicLong
  ): F[Unit] =
    for {
      windowed <- windowMax.poll
      current <- currentLag
      combined = flooredMax(windowed, current).getOrElse(Duration.Zero)
      _ <- Sync[F].delay(backingNanos.set(combined.toNanos))
    } yield ()

  /** Floors the combination rather than each term, because `currentLag` is floored nowhere else. */
  private def flooredMax(a: Option[FiniteDuration], b: Option[FiniteDuration]): Option[FiniteDuration] =
    (a ++ b).reduceOption(_ max _).map(_.max(Duration.Zero))

  /**
   * @param backingNanos
   *   Micrometer polls a gauge synchronously, so the refreshed value is handed over through this
   * @param observations
   *   See [[Entries.lagGauge]]
   */
  private class PrometheusOnlyLagEntry[F[_]](
    currentLag: F[Option[FiniteDuration]],
    windowMax: DecayingMax[F],
    backingNanos: AtomicLong,
    observations: Counter
  )(implicit F: Sync[F]
  ) extends LagEntry[F]
      with PrometheusRefresh[F] {

    override def record(duration: FiniteDuration): F[Unit] =
      windowMax.record(duration) *>
        F.delay(observations.increment())

    override def refreshPrometheus: F[Unit] =
      refreshLagGauge(currentLag, windowMax, backingNanos)
  }

  /**
   * Do not unify `statsdMax` and `windowMax`: only `snapshotAndReset` may consume a maximum, so a
   * prometheus scrape must never drain the peak that statsd has not yet reported.
   *
   * @param statsdMax
   *   Tracks the peak duration within each statsd reporting period
   * @param currentLag
   *   External source of "true" lag, re-evaluated on every statsd report and every scrape. Must not
   *   raise; see [[Entries.lagGauge]]
   * @param windowMax
   *   Recent peak for prometheus, which survives being read but not the passage of time
   * @param backingNanos
   *   Micrometer polls a gauge synchronously, so the refreshed value is handed over through this
   * @param observations
   *   See [[Entries.lagGauge]]
   */
  private class PrometheusAndStatsdLagEntry[F[_]](
    val name: String,
    statsdMax: Ref[F, Option[FiniteDuration]],
    currentLag: F[Option[FiniteDuration]],
    windowMax: DecayingMax[F],
    backingNanos: AtomicLong,
    observations: Counter
  )(implicit F: Sync[F]
  ) extends InternalEntry[F]
      with LagEntry[F]
      with PrometheusRefresh[F] {

    override def record(duration: FiniteDuration): F[Unit] =
      recordStatsdMax(statsdMax, duration) *>
        windowMax.record(duration) *>
        F.delay(observations.increment())

    override def snapshotAndReset: F[Option[KVMetric]] =
      for {
        recorded <- statsdMax.getAndSet(None)
        alternative <- currentLag
        combined = flooredMax(recorded, alternative)
      } yield combined.map(statsdDurationMetric(name, _))

    override def refreshPrometheus: F[Unit] =
      refreshLagGauge(currentLag, windowMax, backingNanos)
  }

  private[runtime] case class KVMetric(
    key: String,
    value: String,
    metricType: MetricType
  )

  private[runtime] sealed trait MetricType {
    def render: Char
  }

  private[runtime] object MetricType {
    case object Gauge extends MetricType { def render = 'g' }
    case object Count extends MetricType { def render = 'c' }
  }

  /**
   * Unit suffixes an application must not put in a metric name.
   *
   * The unit is a property of the protocol, not of the metric: statsd carries millis and prometheus
   * carries seconds, and each path appends its own suffix. Rejecting these at registration means an
   * application which has not been migrated fails at startup, instead of silently renaming a statsd
   * metric that production alerting depends on.
   */
  private val forbiddenNameSuffixes: List[String] =
    List("_millis", "_ms", "_msec", "_seconds", "_secs", "_sec", "_s", "_micros", "_us", "_nanos", "_ns")

  /** Statsd carries durations in milliseconds, so its metric names say so. */
  private[runtime] val statsdDurationSuffix: String = "_millis"

  /**
   * Name suffix of a lag gauge's prometheus-only observation counter.
   *
   * Micrometer's prometheus naming convention appends `_total` on top of this, so the scraped
   * series is `<name>_observations_total`.
   */
  private[runtime] val lagObservationsSuffix: String = "_observations"

  /**
   * How long a recorded peak stays visible in the prometheus lag gauge.
   *
   * The window must exceed the prometheus scrape interval, or a peak can expire before any scrape
   * observes it, which puts us back to sampling. With 3 buckets a peak is held for between 2 and 3
   * minutes, clearing a 60-second scrape interval with margin.
   *
   * Erring large is the safe direction: any sustained problem is carried by `currentLag`, so the
   * window maximum's only job is catching transient peaks from batches which completed between two
   * scrapes.
   */
  private val lagDecayWindow: FiniteDuration = 3.minutes
  private val lagDecayBuckets: Int           = 3

  private def validateDurationName[F[_]: Sync](name: String): F[Unit] =
    forbiddenNameSuffixes.find(name.endsWith) match {
      case Some(suffix) =>
        Sync[F].raiseError[Unit] {
          new IllegalArgumentException(
            s"Metric name '$name' must not end with '$suffix'. Pass a bare name, e.g. 'latency'. " +
              "The unit suffix is added per protocol: '_millis' for statsd and '_seconds' for prometheus."
          )
        }
      case None =>
        Sync[F].unit
    }

  /**
   * Rejected at registration for the same reason as a unit-suffixed name: a nonsense value should
   * fail there rather than derive a degenerate bucket range which nobody notices until they need
   * the quantiles.
   */
  private def validateExpectedLatency[F[_]: Sync](name: String, expectedLatency: FiniteDuration): F[Unit] =
    if (expectedLatency <= Duration.Zero)
      Sync[F].raiseError[Unit] {
        new IllegalArgumentException(
          s"Timer '$name' must not be registered with a non-positive expectedLatency of $expectedLatency. " +
            "Pass roughly where this metric's values are expected to sit, e.g. 2.seconds. " +
            "The histogram's bucket range is derived from it, from a tenth of it up to a hundred times it."
        )
      }
    else
      Sync[F].unit

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

  private[runtime] trait Reporter[F[_]] {
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
