/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.testing.specs2.CatsEffect
import cats.effect.IO
import org.specs2.Specification

import scala.concurrent.duration._

class MetricsSpec extends Specification with CatsEffect {

  def is = s2"""
  Metrics.build should:
    Counter:
      Increment a counter and reflect it in scrape output $counter1
      Accumulate multiple counter additions in scrape output $counter2
    Gauge:
      Set a gauge value and reflect it in scrape output $gauge1
      Reflect the latest gauge value in scrape output $gauge2
    Timer:
      Record a duration and reflect it in scrape output $timer1
    Scrape:
      Include JVM metrics in scrape output $scrape1
      Include prometheus tags in scrape output when configured $scrape2
      Include multiple registered metrics in scrape output $scrape3
  """

  private val noStatsd        = None
  private val emptyPrometheus = Metrics.PrometheusConfig(tags = Map.empty)

  def counter1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total")
        _ <- counter.add(5)
        scraped <- entries.scrape
      } yield scraped must contain("test_events_total")
    }

  def counter2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        counter <- entries.counter("test_events_total")
        _ <- counter.add(3)
        _ <- counter.add(7)
        scraped <- entries.scrape
      } yield scraped must contain("10.0")
    }

  def gauge1 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size")
        _ <- gauge.set(42)
        scraped <- entries.scrape
      } yield scraped must contain("42.0")
    }

  def gauge2 =
    Metrics.build[IO](noStatsd, emptyPrometheus).use { entries =>
      for {
        gauge <- entries.gauge("test_batch_size")
        _ <- gauge.set(10)
        _ <- gauge.set(25)
        scraped <- entries.scrape
      } yield scraped must contain("25.0")
    }

  def timer1 =
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

}
