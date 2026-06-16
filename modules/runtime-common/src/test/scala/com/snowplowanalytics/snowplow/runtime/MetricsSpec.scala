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
import io.micrometer.prometheusmetrics.{PrometheusConfig => MicrometerPrometheusConfig, PrometheusMeterRegistry}
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
      Send the alternative maximum every cycle when timer was never recorded but alternativeMaximum is provided $reportTimer1
      Send the max recorded duration once, then stop on quiet cycles $reportTimer2
      Not include a timer with no recordings and no alternativeMaximum $reportTimer3
      Send the larger of recorded and alternativeMaximum when both are present $reportTimer4
  """

  private val noStatsd        = None
  private val emptyPrometheus = Metrics.PrometheusConfig(tags = Map.empty)

  def scrapeCounter1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total")
        _ <- counter.add(5)
        scraped <- entries.scrape
      } yield scraped must contain("test_events_total")
    }

  def scrapeCounter2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total")
        _ <- counter.add(3)
        _ <- counter.add(7)
        scraped <- entries.scrape
      } yield scraped must contain("10.0")
    }

  def scrapeGauge1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size")
        _ <- gauge.set(42)
        scraped <- entries.scrape
      } yield scraped must contain("42.0")
    }

  def scrapeGauge2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size")
        _ <- gauge.set(10)
        _ <- gauge.set(25)
        scraped <- entries.scrape
      } yield scraped must contain("25.0")
    }

  def scrapeTimer1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        timer <- entries.timer("test_latency", IO.pure(None))
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
        counter <- entries.counter("tagged_counter_total")
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
        counter <- entries.counter("multi_counter_total")
        gauge <- entries.gauge("multi_gauge")
        timer <- entries.timer("multi_timer", IO.pure(None))
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
      captured <- Ref[IO].of(List.empty[List[Metrics.KVMetric]])
      capturingReporter = new Metrics.Reporter[IO] {
                            def report(metrics: List[Metrics.KVMetric]): IO[Unit] =
                              captured.update(metrics :: _)
                          }
      entries = Metrics.createEntries[IO](registry, registered, List(capturingReporter), period)
      result <- entries.report.compile.drain.background.use(_ => body(entries, captured))
    } yield result

  def reportCounter = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        counter <- entries.counter("counter")
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
        gauge <- entries.gauge("gauge")
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

  def reportTimer1 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.timer("alt_only_timer", IO.pure(Some(5.seconds)))
        _ <- IO.sleep(period * 3.5)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "alt_only_timer")
        List(
          matches must haveSize(3),
          matches.forall(_.value == "5000") must beTrue,
          matches.forall(_.metricType == Metrics.MetricType.Gauge) must beTrue
        ).reduce(_ and _)
      }
    }
  }

  def reportTimer2 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        timer <- entries.timer("once_timer", IO.pure(None))
        _ <- timer.record(80.millis)
        _ <- timer.record(250.millis)
        _ <- timer.record(40.millis)
        _ <- IO.sleep(period * 5)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "once_timer")
        List(
          matches must haveSize(1),
          matches.head.value === "250",
          matches.head.metricType === Metrics.MetricType.Gauge
        ).reduce(_ and _)
      }
    }
  }

  def reportTimer3 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        _ <- entries.timer("untouched_timer", IO.pure(None))
        _ <- IO.sleep(period * 5)
        cycles <- captured.get
      } yield cycles.flatten.exists(_.key == "untouched_timer") must beFalse
    }
  }

  def reportTimer4 = TestControl.executeEmbed {
    withReporter { (entries, captured) =>
      for {
        timer <- entries.timer("combined_timer", IO.pure(Some(5.seconds)))
        _ <- timer.record(80.millis)
        _ <- IO.sleep(period * 1.5)
        _ <- timer.record(8.seconds)
        _ <- IO.sleep(period * 2)
        cycles <- captured.get
      } yield {
        val matches = cycles.flatten.filter(_.key == "combined_timer")
        List(
          matches.map(_.value) === List("5000", "8000", "5000"),
          matches.forall(_.metricType == Metrics.MetricType.Gauge) must beTrue
        ).reduce(_ and _)
      }
    }
  }

}
