/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.{IO, Ref}
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import cats.syntax.traverse._
import io.micrometer.core.instrument.MockClock
import io.micrometer.prometheusmetrics.{PrometheusConfig => MicrometerPrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.specs2.Specification

import scala.concurrent.duration._

class MetricsSpec extends Specification with CatsEffect {

  def is = s2"""
  Metrics.build should:
    Counter:
      Increment a counter and reflect it in scrape output $scrapeCounter1
      Accumulate multiple counter additions in scrape output $scrapeCounter2
    Gauge:
      Set a gauge value and reflect it in scrape output $scrapeGauge1
      Reflect the latest gauge value in scrape output $scrapeGauge2
    Timer:
      Record a duration and reflect it in scrape output $scrapeTimer1
    Scrape:
      Include JVM metrics in scrape output $scrape1
      Include prometheus tags in scrape output when configured $scrape2
      Include multiple registered metrics in scrape output $scrape3

  Metrics report should:
    Counter:
      Emit 0 when quiet and the accumulated count after updates, resetting between cycles $reportCounter
    Gauge:
      Emit the max value once after each burst of sets, stay silent on quiet cycles $reportGauge
    Timer:
      Send the max recorded duration once, then stop on quiet cycles $reportTimer1
      Not include a timer with no recordings $reportTimer2
    Lag gauge:
      Send the current lag every cycle when nothing was recorded $reportLag1
      Send the larger of recorded and current lag when both are present $reportLag2
      Still send a recorded peak which a prometheus scrape has already read $reportLag3
      Send zero rather than a negative when currentLag is negative $reportLag4

  Metrics with a PrometheusOnly destination should:
    Appear in scrape output but never be reported to statsd, for a counter $prometheusOnlyCounter
    Appear in scrape output but never be reported to statsd, for a gauge $prometheusOnlyGauge
    Never be reported to statsd, for a timer $prometheusOnlyTimer
    Not prevent a PrometheusAndStatsd metric registered alongside from reporting $prometheusOnlyMixed
    Keep reporting a gauge value after the caller's entry is garbage collected $prometheusOnlyGaugeSurvivesGc

  Metrics name validation should:
    Reject a lag gauge whose name carries a time unit $rejectLagName
    Reject a timer whose name carries a time unit $rejectTimerName
    Reject a PrometheusOnly timer whose name carries a time unit $rejectPrometheusOnlyName
    Accept a bare duration name $acceptBareName
    Not constrain counter or gauge names $allowOtherNames

  Metrics prometheus lag gauge should:
    Expose a seconds-suffixed gauge from the first scrape, before anything is recorded $lagScrape1
    Reflect a recorded duration in seconds $lagScrape2
    Reflect currentLag when nothing has been recorded $lagScrape3
    Report the larger of the recorded maximum and currentLag $lagScrape4
    Return the same value when scraped twice $lagScrape5
    Stop reporting a recorded peak once the decay window has elapsed $lagScrape6
    Keep reporting a growing currentLag, without decaying, while an output is stuck $lagDoesNotDecayWhileOutputStuck
    Still be refreshed when registered as PrometheusOnly $lagScrape7
    Publish zero rather than a negative when currentLag is negative and the window has expired $lagScrape8
    Expose an observations counter from the first scrape, before anything is recorded $lagObservations1
    Increment the observations counter once per recording $lagObservations2
    Increment the observations counter for a PrometheusOnly lag gauge $lagObservations3
    Never report the observations counter to statsd $lagObservations4

  Metrics prometheus timer should:
    Expose histogram buckets, without an unbounded number of them $timerHistogram1
    Not decay its bucket counts, despite the 60s distribution statistic expiry $timerHistogram2
    Derive different bucket boundaries from different expected latency hints $timerHistogram3
    Bracket the expected latency, a tenth of it below and a hundred times it above $timerHistogram4

  Metrics timer registration should:
    Reject a non-positive expected latency $rejectNonPositiveLatency
  """

  private val noStatsd        = None
  private val emptyPrometheus = Metrics.PrometheusConfig(tags = Map.empty)

  def scrapeCounter1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total", Metrics.Destination.PrometheusAndStatsd)
        _ <- counter.add(5)
        scraped <- entries.scrape
      } yield scraped must contain("test_events_total")
    }

  def scrapeCounter2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total", Metrics.Destination.PrometheusAndStatsd)
        _ <- counter.add(3)
        _ <- counter.add(7)
        scraped <- entries.scrape
      } yield scraped must contain("10.0")
    }

  def scrapeGauge1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size", Metrics.Destination.PrometheusAndStatsd)
        _ <- gauge.set(42)
        scraped <- entries.scrape
      } yield scraped must contain("42.0")
    }

  def scrapeGauge2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size", Metrics.Destination.PrometheusAndStatsd)
        _ <- gauge.set(10)
        _ <- gauge.set(25)
        scraped <- entries.scrape
      } yield scraped must contain("25.0")
    }

  def scrapeTimer1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        timer <- entries.timer("test_latency", 500.millis, Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(500.millis)
        scraped <- entries.scrape
      } yield scraped must contain("test_latency")
    }

  def scrape1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        scraped <- entries.scrape
      } yield List(
        scraped must contain("jvm_memory"),
        scraped must contain("jvm_threads")
      ).reduce(_ and _)
    }

  def scrape2 = {
    val prometheusConfig = Metrics.PrometheusConfig(tags = Map("env" -> "test", "app" -> "loader"))
    Metrics.build[IO](noStatsd, prometheusConfig).use { entries =>
      for {
        counter <- entries.counter("tagged_counter_total", Metrics.Destination.PrometheusAndStatsd)
        _ <- counter.add(1)
        scraped <- entries.scrape
      } yield List(
        scraped must contain("env"),
        scraped must contain("test"),
        scraped must contain("app"),
        scraped must contain("loader")
      ).reduce(_ and _)
    }
  }

  def scrape3 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("multi_counter_total", Metrics.Destination.PrometheusAndStatsd)
        gauge <- entries.gauge("multi_gauge", Metrics.Destination.PrometheusAndStatsd)
        timer <- entries.timer("multi_timer", 100.millis, Metrics.Destination.PrometheusAndStatsd)
        _ <- counter.add(1)
        _ <- gauge.set(99)
        _ <- timer.record(100.millis)
        scraped <- entries.scrape
      } yield List(
        scraped must contain("multi_counter_total"),
        scraped must contain("multi_gauge"),
        scraped must contain("multi_timer")
      ).reduce(_ and _)
    }

  // ---------- Statsd report cycle (in-memory reporter + virtual time) ----------

  private val period = 1.second

  private def withReporter[A](
    body: (Metrics.Entries[IO], Ref[IO, List[List[Metrics.KVMetric]]]) => IO[A]
  ): IO[A] =
    for {
      registry <- IO.delay(new PrometheusMeterRegistry(MicrometerPrometheusConfig.DEFAULT))
      registered <- Ref[IO].of(List.empty[Metrics.InternalEntry[IO]])
      refreshables <- Ref[IO].of(List.empty[Metrics.PrometheusRefresh[IO]])
      captured <- Ref[IO].of(List.empty[List[Metrics.KVMetric]])
      capturingReporter = new Metrics.Reporter[IO] {
                            def report(metrics: List[Metrics.KVMetric]): IO[Unit] =
                              captured.update(metrics :: _)
                          }
      entries = Metrics.createEntries[IO](registry, registered, refreshables, List(capturingReporter), period)
      result <- entries.report.compile.drain.background.use(_ => body(entries, captured))
    } yield result

  def reportCounter = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        counter <- entries.counter("counter", Metrics.Destination.PrometheusAndStatsd)
        _ <- IO.sleep(period * 1.5) // tick at t=1: emit "0" (untouched)
        _ <- counter.add(3)
        _ <- counter.add(4)
        _ <- IO.sleep(period * 2) // tick at t=2: emit "7"; tick at t=3: emit "0"
        _ <- counter.add(5)
        _ <- IO.sleep(period * 4) // tick at t=4: emit "5"; tick at t=5, 6, 7: emit "0"
        cycles <- captured.get
      } yield {
        val matches = cycles.reverse.flatten.filter(_.key == "counter")
        List(
          matches.map(_.value) === List("0", "7", "0", "5", "0", "0", "0"),
          matches.forall(_.metricType == Metrics.MetricType.Count) must beTrue
        ).reduce(_ and _)
      }
    }
  }

  def reportGauge = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        gauge <- entries.gauge("gauge", Metrics.Destination.PrometheusAndStatsd)
        _ <- IO.sleep(period * 1.5) // tick at t=1: untouched, not emitted
        _ <- gauge.set(20)
        _ <- gauge.set(42)
        _ <- gauge.set(15)
        _ <- IO.sleep(period * 2) // tick at t=2: emit "42"; tick at t=3: not emitted
        _ <- gauge.set(8)
        _ <- gauge.set(12)
        _ <- IO.sleep(period * 2) // tick at t=4: emit "12"; tick at t=5: not emitted
        cycles <- captured.get
      } yield {
        val matches = cycles.reverse.flatten.filter(_.key == "gauge")
        List(
          matches.map(_.value) === List("42", "12"),
          matches.forall(_.metricType == Metrics.MetricType.Gauge) must beTrue
        ).reduce(_ and _)
      }
    }
  }

  def reportLag1 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.lagGauge("alt_only_lag", IO.pure(Some(5.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "alt_only_lag_millis")
        List(
          matches must haveSize(3),
          matches.forall(_.value == "5000") must beTrue,
          matches.forall(_.metricType == Metrics.MetricType.Gauge) must beTrue
        ).reduce(_ and _)
      }
    }
  }

  def reportTimer1 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        timer <- entries.timer("once_timer", 200.millis, Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(80.millis)
        _ <- timer.record(250.millis)
        _ <- timer.record(40.millis)
        _ <- IO.sleep(period * 5)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "once_timer_millis")
        List(
          matches must haveSize(1),
          matches.head.value === "250",
          matches.head.metricType === Metrics.MetricType.Gauge
        ).reduce(_ and _)
      }
    }
  }

  def reportTimer2 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.timer("untouched_timer", 1.second, Metrics.Destination.PrometheusAndStatsd)
        _ <- IO.sleep(period * 5)
        cycles <- captured.get
      } yield cycles.flatten.map(_.key).exists(_.startsWith("untouched_timer")) must beFalse
    }
  }

  def reportLag2 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        lag <- entries.lagGauge("combined_lag", IO.pure(Some(5.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(80.millis)
        _ <- IO.sleep(period * 1.5)
        _ <- lag.record(8.seconds)
        _ <- IO.sleep(period * 2)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "combined_lag_millis")
        List(
          matches.map(_.value) === List("5000", "8000", "5000"),
          matches.forall(_.metricType == Metrics.MetricType.Gauge) must beTrue
        ).reduce(_ and _)
      }
    }
  }

  /**
   * Pins the "do not unify `statsdMax` and `windowMax`" warning on `PrometheusAndStatsdLagEntry`.
   */
  def reportLag3 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        lag <- entries.lagGauge("scraped_lag", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(5.seconds)
        _ <- entries.scrape
        _ <- IO.sleep(period * 1.5) // one report cycle, after the scrape
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "scraped_lag_millis")
        List(
          matches must haveSize(1),
          matches.head.value === "5000"
        ).reduce(_ and _)
      }
    }
  }

  /**
   * The one deliberate change to statsd's output: a negative `currentLag` reaches
   * `snapshotAndReset` unfloored, and a negative number of milliseconds of lag is meaningless.
   */
  def reportLag4 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.lagGauge("negative_lag", IO.pure(Some(-3.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- IO.sleep(period * 2.5)
        cycles <- captured.get
      } yield cycles.flatten.filter(_.key == "negative_lag_millis").map(_.value) === List("0", "0")
    }
  }

  // ---------- PrometheusOnly destination ----------

  def prometheusOnlyCounter = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        counter <- entries.counter("cheap_counter", Metrics.Destination.PrometheusOnly)
        canary <- entries.counter("canary", Metrics.Destination.PrometheusAndStatsd)
        _ <- counter.add(9)
        _ <- canary.add(1)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
        scraped <- entries.scrape
      } yield List(
        scraped must contain("cheap_counter"),
        scraped must contain("9.0"),
        cycles.flatten.exists(_.key == "canary") must beTrue,
        cycles.flatten.exists(_.key == "cheap_counter") must beFalse
      ).reduce(_ and _)
    }
  }

  def prometheusOnlyGauge = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        gauge <- entries.gauge("cheap_gauge", Metrics.Destination.PrometheusOnly)
        canary <- entries.counter("canary", Metrics.Destination.PrometheusAndStatsd)
        _ <- gauge.set(42)
        _ <- canary.add(1)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
        scraped <- entries.scrape
      } yield List(
        scraped must contain("cheap_gauge"),
        scraped must contain("42.0"),
        cycles.flatten.exists(_.key == "canary") must beTrue,
        cycles.flatten.exists(_.key == "cheap_gauge") must beFalse
      ).reduce(_ and _)
    }
  }

  def prometheusOnlyTimer = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        timer <- entries.timer("cheap_timer", 100.millis, Metrics.Destination.PrometheusOnly)
        canary <- entries.counter("canary", Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(80.millis)
        _ <- canary.add(1)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
        scraped <- entries.scrape
      } yield List(
        scraped must contain("cheap_timer"),
        cycles.flatten.exists(_.key == "canary") must beTrue,
        cycles.flatten.exists(_.key == "cheap_timer") must beFalse
      ).reduce(_ and _)
    }
  }

  def prometheusOnlyMixed = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        cheap <- entries.counter("mixed_cheap", Metrics.Destination.PrometheusOnly)
        expensive <- entries.counter("mixed_expensive", Metrics.Destination.PrometheusAndStatsd)
        _ <- cheap.add(1)
        _ <- expensive.add(2)
        _ <- IO.sleep(period * 1.5)
        cycles <- captured.get
      } yield {
        val reported = cycles.flatten
        List(
          reported.filter(_.key == "mixed_expensive").map(_.value) === List("2"),
          reported.exists(_.key == "mixed_cheap") must beFalse
        ).reduce(_ and _)
      }
    }
  }

  /**
   * Micrometer holds only a weak reference to the object backing a gauge, so a PrometheusOnly gauge
   * must keep itself alive without relying on the list of statsd entries.
   *
   * Deliberately retains no reference to the entry, so nothing but the registry can keep the gauge
   * alive. If the GC does not run, this passes without proving anything - it can never fail
   * spuriously.
   */
  private def registerGaugeAndDropEntry(entries: Metrics.Entries[IO], name: String): IO[Unit] =
    entries.gauge(name, Metrics.Destination.PrometheusOnly).flatMap(_.set(42)).void

  /**
   * Note the System.gc() is process-wide and specs2 runs examples concurrently, so if a weakly
   * referenced meter is ever reintroduced the symptom may well surface as an unexplained failure in
   * one of the other examples rather than in this one.
   */
  def prometheusOnlyGaugeSurvivesGc =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        _ <- registerGaugeAndDropEntry(entries, "gc_gauge")
        _ <- IO.delay(System.gc()) *> IO.delay(System.gc())
        scraped <- entries.scrape
      } yield scraped.linesIterator.filter(_.startsWith("gc_gauge")).mkString must contain("42.0")
    }

  // ---------- Name validation ----------

  private val unitSuffixedNames =
    List(
      "latency_millis",
      "latency_ms",
      "latency_msec",
      "latency_seconds",
      "latency_secs",
      "latency_sec",
      "latency_s",
      "latency_micros",
      "latency_us",
      "latency_nanos",
      "latency_ns"
    )

  def rejectLagName =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      unitSuffixedNames
        .traverse(name => entries.lagGauge(name, IO.pure(None), Metrics.Destination.PrometheusAndStatsd).attempt)
        .map(_.map(_ must beLeft(beAnInstanceOf[IllegalArgumentException])).reduce(_ and _))
    }

  def rejectTimerName =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      unitSuffixedNames
        .traverse(name => entries.timer(name, 1.second, Metrics.Destination.PrometheusAndStatsd).attempt)
        .map(_.map(_ must beLeft(beAnInstanceOf[IllegalArgumentException])).reduce(_ and _))
    }

  def rejectPrometheusOnlyName =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      entries
        .timer("latency_millis", 1.second, Metrics.Destination.PrometheusOnly)
        .attempt
        .map(_ must beLeft(beAnInstanceOf[IllegalArgumentException]))
    }

  def acceptBareName =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd).attempt
        timer <- entries.timer("e2e_latency", 2.seconds, Metrics.Destination.PrometheusAndStatsd).attempt
      } yield (lag.isRight && timer.isRight) must beTrue
    }

  def allowOtherNames =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("events_seconds", Metrics.Destination.PrometheusAndStatsd).attempt
        gauge <- entries.gauge("batch_millis", Metrics.Destination.PrometheusAndStatsd).attempt
      } yield (counter.isRight && gauge.isRight) must beTrue
    }

  // ---------- Prometheus lag gauge ----------

  /** Value of a named series in prometheus text format, ignoring comments and any tags. */
  private def metricValue(scraped: String, name: String): Option[Double] =
    scraped.linesIterator
      .filterNot(_.startsWith("#"))
      .collectFirst {
        case line if line.startsWith(s"$name ") || line.startsWith(s"$name{") =>
          line.trim.split(" ").last.toDouble
      }

  /** Every bucket count of a named histogram, in scrape order. */
  private def bucketCounts(scraped: String, name: String): List[Double] =
    scraped.linesIterator
      .filterNot(_.startsWith("#"))
      .filter(_.startsWith(s"${name}_bucket"))
      .map(_.trim.split(" ").last.toDouble)
      .toList

  /**
   * Every finite `le` boundary of a named histogram, in scrape order.
   *
   * The `+Inf` bucket is dropped, because it is present whatever the expected-value range and so
   * says nothing about the range that was derived.
   */
  private def finiteBoundaries(scraped: String, name: String): List[Double] =
    scraped.linesIterator
      .filterNot(_.startsWith("#"))
      .filter(_.startsWith(s"${name}_bucket"))
      .map(_.split("le=\"")(1).takeWhile(_ != '"'))
      .filterNot(_ == "+Inf")
      .map(_.toDouble)
      .toList

  def lagScrape1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        _ <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        scraped <- entries.scrape
      } yield List(
        metricValue(scraped, "latency_seconds") must beSome(0.0),
        scraped must not(contain("latency_millis"))
      ).reduce(_ and _)
    }

  def lagScrape2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(500.millis)
        scraped <- entries.scrape
      } yield metricValue(scraped, "latency_seconds") must beSome(0.5)
    }

  def lagScrape3 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        _ <- entries.lagGauge("latency", IO.pure(Some(7.seconds)), Metrics.Destination.PrometheusAndStatsd)
        scraped <- entries.scrape
      } yield metricValue(scraped, "latency_seconds") must beSome(7.0)
    }

  def lagScrape4 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(Some(7.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(2.seconds)
        smaller <- entries.scrape
        _ <- lag.record(9.seconds)
        larger <- entries.scrape
      } yield List(
        metricValue(smaller, "latency_seconds") must beSome(7.0),
        metricValue(larger, "latency_seconds") must beSome(9.0)
      ).reduce(_ and _)
    }

  def lagScrape5 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(4.seconds)
        first <- entries.scrape
        second <- entries.scrape
      } yield List(
        metricValue(first, "latency_seconds") must beSome(4.0),
        metricValue(second, "latency_seconds") must beSome(4.0)
      ).reduce(_ and _)
    }

  /**
   * The timings bracket the retention `lagDecayWindow` and `lagDecayBuckets` promise, without
   * relying on where the recording falls within a bucket: a peak is held for at least `window -
   * window / bucketCount` = 2 minutes and at most `window` = 3 minutes.
   */
  def lagScrape6 = TestControl.executeEmbed {
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(4.seconds)
        before <- entries.scrape
        _ <- IO.sleep(119.seconds)
        within <- entries.scrape
        _ <- IO.sleep(62.seconds)
        after <- entries.scrape
      } yield List(
        metricValue(before, "latency_seconds") must beSome(4.0),
        metricValue(within, "latency_seconds") must beSome(4.0),
        metricValue(after, "latency_seconds") must beSome(0.0)
      ).reduce(_ and _)
    }
  }

  /**
   * The question this pins down: when the application is stuck on a downstream write, does the
   * gauge decay to a healthy-looking zero?
   *
   * It does not. `currentStreamLatency` computes `now - tstamp` freshly on every call from the
   * in-flight batch's timestamp, so it keeps growing for as long as the outage lasts, and
   * `refreshLagGauge` maxes the decaying window peak against it. Here nothing is recorded after the
   * initial 4 seconds and virtual time runs far beyond the decay window, so the window peak has
   * certainly expired - anything the scrape still reports comes from `currentLag`.
   */
  def lagDoesNotDecayWhileOutputStuck = TestControl.executeEmbed {
    withReporter { (entries, _) =>
      for {
        stuckSince <- IO.realTime
        currentLag = IO.realTime.map(now => Option(now - stuckSince))
        lag <- entries.lagGauge("latency", currentLag, Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(4.seconds)
        _ <- IO.sleep(5.minutes)
        afterWindow <- entries.scrape
        _ <- IO.sleep(5.minutes)
        stillStuck <- entries.scrape
      } yield List(
        metricValue(afterWindow, "latency_seconds") must beSome(300.0),
        metricValue(stillStuck, "latency_seconds") must beSome(600.0)
      ).reduce(_ and _)
    }
  }

  def lagScrape7 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.lagGauge("cheap_lag", IO.pure(Some(7.seconds)), Metrics.Destination.PrometheusOnly)
        canary <- entries.counter("canary", Metrics.Destination.PrometheusAndStatsd)
        _ <- canary.add(1)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
        scraped <- entries.scrape
      } yield List(
        metricValue(scraped, "cheap_lag_seconds") must beSome(7.0),
        cycles.flatten.exists(_.key == "canary") must beTrue,
        cycles.flatten.map(_.key).exists(_.startsWith("cheap_lag")) must beFalse
      ).reduce(_ and _)
    }
  }

  /**
   * `currentLag` never passes through [[DecayingMax]], so its own floor does not cover it. Here the
   * window peak has expired, leaving the negative as the maximum of what is left.
   */
  def lagScrape8 = TestControl.executeEmbed {
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(Some(-3.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(4.seconds)
        _ <- IO.sleep(4.minutes) // beyond lagDecayWindow, so nothing is left to mask the negative
        scraped <- entries.scrape
      } yield metricValue(scraped, "latency_seconds") must beSome(0.0)
    }
  }

  /**
   * These examples assert the literal scraped series name, `_total` included, because that suffix
   * is micrometer's doing rather than ours. What the counter is for is in
   * `Metrics.Entries.lagGauge`.
   */
  def lagObservations1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        _ <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        scraped <- entries.scrape
      } yield metricValue(scraped, "latency_observations_total") must beSome(0.0)
    }

  def lagObservations2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("latency", IO.pure(None), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(1.second)
        afterOne <- entries.scrape
        _ <- lag.record(Duration.Zero)
        _ <- lag.record(Duration.Zero)
        afterThree <- entries.scrape
      } yield List(
        metricValue(afterOne, "latency_observations_total") must beSome(1.0),
        metricValue(afterThree, "latency_observations_total") must beSome(3.0)
      ).reduce(_ and _)
    }

  def lagObservations3 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        lag <- entries.lagGauge("cheap_lag", IO.pure(None), Metrics.Destination.PrometheusOnly)
        _ <- lag.record(1.second)
        _ <- lag.record(2.seconds)
        scraped <- entries.scrape
      } yield metricValue(scraped, "cheap_lag_observations_total") must beSome(2.0)
    }

  def lagObservations4 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        lag <- entries.lagGauge("statsd_lag", IO.pure(Some(3.seconds)), Metrics.Destination.PrometheusAndStatsd)
        _ <- lag.record(1.second)
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
        scraped <- entries.scrape
      } yield {
        val keys = cycles.flatten.map(_.key)
        List(
          metricValue(scraped, "statsd_lag_observations_total") must beSome(1.0),
          keys must contain("statsd_lag_millis"),
          keys.exists(_.contains("observations")) must beFalse
        ).reduce(_ and _)
      }
    }
  }

  // ---------- Prometheus timer histogram ----------

  /**
   * These build the registry directly so that micrometer's own clock can be advanced, which
   * `TestControl` cannot do.
   */
  private def withMicrometerClock[A](body: (Metrics.Entries[IO], MockClock) => IO[A]): IO[A] = {
    val clock = new MockClock
    for {
      registry <- IO.delay {
                    new PrometheusMeterRegistry(MicrometerPrometheusConfig.DEFAULT, new PrometheusRegistry, clock)
                  }
      registered <- Ref[IO].of(List.empty[Metrics.InternalEntry[IO]])
      refreshables <- Ref[IO].of(List.empty[Metrics.PrometheusRefresh[IO]])
      entries = Metrics.createEntries[IO](
                  registry,
                  registered,
                  refreshables,
                  List.empty[Metrics.Reporter[IO]],
                  1.minute
                )
      result <- body(entries, clock)
    } yield result
  }

  def timerHistogram1 =
    withMicrometerClock { (entries, _) =>
      for {
        timer <- entries.timer("e2e_latency", 2.seconds, Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(2.seconds)
        scraped <- entries.scrape
      } yield List(
        bucketCounts(scraped, "e2e_latency_seconds") must not(beEmpty),
        bucketCounts(scraped, "e2e_latency_seconds").reduceOption(_ max _) must beSome(beGreaterThan(0.0)),
        // The 2-second hint gives a 200ms..200s range and 48 buckets on micrometer 1.14.4: 47
        // finite boundaries from le="0.2" to le="200.0", plus +Inf. A 1000x range comes out at 48
        // whatever the hint, so this bound does not have to be revisited per call site. The exact
        // number is not the point; the point is that a future micrometer bump cannot inflate the
        // per-pod series count unnoticed. Deliberately a bound rather than an equality, so a bump
        // fails loudly here instead of silently multiplying the series.
        bucketCounts(scraped, "e2e_latency_seconds").size must beLessThan(60),
        // The load-bearing half of the range: the recorded value must land in a finite bucket, or
        // histogram_quantile has nothing to interpolate and every quantile reads back as the top
        // boundary.
        finiteBoundaries(scraped, "e2e_latency_seconds").reduceOption(_ max _) must beSome(beGreaterThan(2.0))
      ).reduce(_ and _)
    }

  /**
   * The bucket counts are cumulative, despite the 60-second `distributionStatisticExpiry` on the
   * builder, because the prometheus registry overrides that expiry for histogram buckets. Ten
   * virtual minutes on micrometer's own clock is well past the expiry.
   */
  def timerHistogram2 =
    withMicrometerClock { (entries, clock) =>
      for {
        timer <- entries.timer("e2e_latency", 2.seconds, Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(2.seconds)
        before <- entries.scrape
        _ <- IO.delay(clock.add(java.time.Duration.ofMinutes(10)))
        after <- entries.scrape
      } yield List(
        bucketCounts(before, "e2e_latency_seconds") must not(beEmpty),
        bucketCounts(after, "e2e_latency_seconds") === bucketCounts(before, "e2e_latency_seconds")
      ).reduce(_ and _)
    }

  /**
   * The parameter must demonstrably reach micrometer. Without this, `timer` could ignore
   * `expectedLatency` entirely - going back to a hardcoded range, or dropping the range and
   * inheriting micrometer's pre-seeded 1ms..30s - and every other example in the suite would still
   * pass.
   */
  def timerHistogram3 =
    withMicrometerClock { (entries, _) =>
      for {
        fast <- entries.timer("fast_latency", 2.seconds, Metrics.Destination.PrometheusOnly)
        slow <- entries.timer("slow_latency", 2.minutes, Metrics.Destination.PrometheusOnly)
        _ <- fast.record(2.seconds)
        _ <- slow.record(2.minutes)
        scraped <- entries.scrape
      } yield {
        val fastBoundaries = finiteBoundaries(scraped, "fast_latency_seconds")
        val slowBoundaries = finiteBoundaries(scraped, "slow_latency_seconds")
        List(
          fastBoundaries must not(beEmpty),
          slowBoundaries must not(beEmpty),
          fastBoundaries must not(be_===(slowBoundaries)),
          // Not merely different, but different in the direction the hint asks for, at both ends
          fastBoundaries.min must beLessThan(slowBoundaries.min),
          fastBoundaries.max must beLessThan(slowBoundaries.max)
        ).reduce(_ and _)
      }
    }

  /**
   * The derived endpoints for a known hint: `expectedLatency / 10` and `expectedLatency * 100`.
   *
   * Micrometer snaps a boundary to its own standard set, so these are ratio bounds around the
   * derived values rather than equalities - close enough to prove the arithmetic, loose enough to
   * survive micrometer choosing a slightly different neighbouring bucket.
   */
  def timerHistogram4 =
    withMicrometerClock { (entries, _) =>
      for {
        timer <- entries.timer("bracketed_latency", 2.seconds, Metrics.Destination.PrometheusAndStatsd)
        _ <- timer.record(2.seconds)
        scraped <- entries.scrape
      } yield {
        val boundaries = finiteBoundaries(scraped, "bracketed_latency_seconds")
        List(
          boundaries must not(beEmpty),
          // 2s / 10 = 200ms
          boundaries.min must beCloseTo(0.2, 0.05),
          // 2s * 100 = 200s
          boundaries.max must beCloseTo(200.0, 20.0)
        ).reduce(_ and _)
      }
    }

  def rejectNonPositiveLatency =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        zero <- entries.timer("zero_latency", Duration.Zero, Metrics.Destination.PrometheusAndStatsd).attempt
        negative <- entries.timer("negative_latency", -1.second, Metrics.Destination.PrometheusAndStatsd).attempt
        positive <- entries.timer("positive_latency", 1.second, Metrics.Destination.PrometheusAndStatsd).attempt
      } yield List(
        zero must beLeft(beAnInstanceOf[IllegalArgumentException]),
        negative must beLeft(beAnInstanceOf[IllegalArgumentException]),
        positive.isRight must beTrue
      ).reduce(_ and _)
    }

}
