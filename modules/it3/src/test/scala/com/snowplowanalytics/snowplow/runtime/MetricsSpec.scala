/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import java.net.Socket
import scala.concurrent.duration.DurationInt
import cats.effect.{IO, Resource}
import cats.effect.testing.specs2.CatsResource
import org.specs2.mutable.SpecificationLike
import retry.syntax.all._
import retry.RetryPolicies

class MetricsSpec extends CatsResource[IO, StatsdAPI[IO]] with SpecificationLike {

  override protected val ResourceTimeout = 1.minute

  override val resource: Resource[IO, StatsdAPI[IO]] =
    for {
      statsd <- Statsd.resource(classOf[MetricsSpec].getSimpleName)
      socket <- Resource.eval(IO.blocking(new Socket(statsd.getHost(), statsd.getMappedPort(8126))))
      statsdApi <- StatsdAPI.resource[IO](socket)
    } yield statsdApi

  override def is = s2"""
  MetricsSpec should
    deliver counter and gauge metrics to statsd $e1
  """

  def e1 = withResource { statsdApi =>
    val statsdConfig = Metrics.StatsdConfig(
      hostname = "localhost",
      port     = 8125,
      tags     = Map.empty,
      period   = 300.millis,
      prefix   = "snowplow"
    )
    val prometheusConfig = Metrics.PrometheusConfig(tags = Map.empty)

    Metrics.build[IO](Some(statsdConfig), prometheusConfig).use { entries =>
      for {
        counter <- entries.counter("events_count")
        timer <- entries.timer("latency", IO.pure(None))
        _ <- counter.add(100)
        _ <- timer.record(10.seconds)
        f <- entries.report.compile.drain.start
        _ <- IO.sleep(350.millis)
        counters <- statsdApi.getCounters
                      .retryingOnFailures(
                        v => IO.pure(v.contains("snowplow.events_count")),
                        RetryPolicies.constantDelay[IO](10.milliseconds),
                        (v, _) => IO.pure(println(s"Retry fetching metrics. Not ready: $v"))
                      )
        gauges <- statsdApi.getGauges
                    .retryingOnFailures(
                      v => IO.pure(v.contains("snowplow.latency")),
                      RetryPolicies.constantDelay[IO](10.milliseconds),
                      (v, _) => IO.pure(println(s"Retry fetching metrics. Not ready: $v"))
                    )
        _ <- f.cancel
      } yield List(
        counters.get("statsd.metrics_received") must beSome(2),
        counters.get("snowplow.events_count") must beSome(100),
        gauges must haveSize(1),
        gauges.get("snowplow.latency") must beSome(10000)
      ).reduce(_ and _)
    }
  }
}
