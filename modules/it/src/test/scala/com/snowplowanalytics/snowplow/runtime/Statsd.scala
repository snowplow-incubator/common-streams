/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import scala.jdk.CollectionConverters._
import cats.effect.{IO, Resource}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Ports

object Statsd {

  def resource(
    loggerName: String
  ): Resource[IO, GenericContainer[_]] =
    Resource.make {
      val statsd: GenericContainer[_] = new GenericContainer("statsd/statsd:v0.10.1")
      statsd.addExposedPort(8126)
      statsd.setWaitStrategy(Wait.forLogMessage("""^(.*)server is up(.+)$""", 1))
      statsd.withCreateContainerCmdModifier { cmd =>
        val statsPort = 8125
        cmd.withExposedPorts((cmd.getExposedPorts().toList :+ ExposedPort.udp(statsPort)).asJava)
        val ports = cmd.getHostConfig().getPortBindings()
        ports.bind(ExposedPort.udp(statsPort), Ports.Binding.bindPort(statsPort))
        cmd.getHostConfig().withPortBindings(ports)
        ()
      }
      IO(start(statsd, loggerName))
    }(ls => IO.blocking(ls.stop()))

  private def start(statsd: GenericContainer[_], loggerName: String): GenericContainer[_] = {
    statsd.start()
    val logger = LoggerFactory.getLogger(loggerName)
    val logs   = new Slf4jLogConsumer(logger)
    statsd.followOutput(logs)
    statsd
  }
}
