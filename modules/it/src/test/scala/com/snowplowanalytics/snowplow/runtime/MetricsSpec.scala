/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import java.net.Socket
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsResource
import org.specs2.mutable.SpecificationLike
import org.specs2.specification.BeforeAll
import org.testcontainers.containers.GenericContainer
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.api.command.PullImageResultCallback

import retry.syntax.all._
import retry.RetryPolicies
import com.github.dockerjava.api.model.PullResponseItem

class MetricsSpec extends CatsResource[IO, (GenericContainer[_], StatsdAPI[IO])] with SpecificationLike with BeforeAll {

  override def beforeAll(): Unit = {
    // blocking the main thread to fetch before we start creating a container
    DockerClientBuilder
      .getInstance()
      .build()
      .pullImageCmd(Statsd.image)
      .withTag(Statsd.tag)
      .withPlatform("linux/amd64")
      .exec(new PullImageResultCallback() {
        override def onNext(item: PullResponseItem) = {
          println(s"StatsD image: ${item.getStatus()}")
          super.onNext(item)
        }
      })
      .awaitCompletion()
      .onComplete()
    super.beforeAll()
  }

  override val resource: Resource[IO, (GenericContainer[_], StatsdAPI[IO])] =
    for {
      statsd <- Statsd.resource(TestMetrics.getClass.getSimpleName)
      socket <- Resource.eval(IO.blocking(new Socket(statsd.getHost(), statsd.getMappedPort(8126))))
      statsdApi <- StatsdAPI.resource[IO](socket)
    } yield (statsd, statsdApi)

  override def is = s2"""
  MetricsSpec should
    deliver metrics to statsd $e1
  """

  def e1 = withResource { case (statsd @ _, statsdApi) =>
    for {
      t <- TestMetrics.impl
      _ <- t.count(100)
      _ <- t.time(10.seconds)
      f <- t.report.compile.drain.start
      _ <- IO.sleep(150.millis)
      counters <- statsdApi
                    .get(Metrics.MetricType.Count)
                    .retryingOnFailures(
                      v => IO.pure(v.contains("snowplow.counter")),
                      RetryPolicies.constantDelay[IO](10.milliseconds),
                      (v, _) => IO.pure(println(s"Retry fetching metrics. Not ready: $v"))
                    )
      gauges <- statsdApi
                  .get(Metrics.MetricType.Gauge)
                  .retryingOnFailures(
                    v => IO.pure(v.contains("snowplow.timer")),
                    RetryPolicies.constantDelay[IO](10.milliseconds),
                    (v, _) => IO.pure(println(s"Retry fetching metrics. Not ready: $v"))
                  )
      _ <- f.cancel
    } yield List(
      counters.get("statsd.metrics_received") must beSome(2),
      counters.get("snowplow.counter") must beSome(100),
      gauges must haveSize(1),
      gauges.get("snowplow.timer") must beSome(10)
    ).reduce(_ and _)

  }
}

object TestMetrics {

  case class TestMetrics(
    ref: Ref[IO, TestState],
    emptyState: TestState,
    config: Option[Metrics.StatsdConfig]
  ) extends Metrics[IO, TestState](ref, emptyState, config) {
    def count(c: Int)           = ref.update(s => s.copy(counter = s.counter + c))
    def time(t: FiniteDuration) = ref.update(s => s.copy(timer = s.timer + t))
  }

  def impl = Ref[IO]
    .of(TestState.empty)
    .map { ref =>
      TestMetrics(
        ref,
        TestState.empty,
        Some(
          Metrics.StatsdConfig(
            "localhost",
            8125,
            Map.empty,
            100.millis,
            ""
          )
        )
      )
    }

  case class TestState(counter: Int, timer: FiniteDuration) extends Metrics.State {
    override def toKVMetrics: List[Metrics.KVMetric] = List(
      Count(counter),
      Timer(timer)
    )
  }
  object TestState {
    def empty = TestState(0, 0.seconds)
  }

  case class Count(v: Int) extends Metrics.KVMetric {
    val key        = "snowplow.counter"
    val value      = v.toString()
    val metricType = Metrics.MetricType.Count
  }

  case class Timer(v: FiniteDuration) extends Metrics.KVMetric {
    val key        = "snowplow.timer"
    val value      = v.toSeconds.toString()
    val metricType = Metrics.MetricType.Gauge
  }
}
