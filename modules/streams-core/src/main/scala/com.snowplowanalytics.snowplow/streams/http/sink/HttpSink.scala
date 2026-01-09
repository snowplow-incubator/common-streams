/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.http.sink

import cats.effect.implicits._
import cats.effect.{Async, Resource}
import cats.implicits._

import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import org.http4s.{MediaType, Method, Request}

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}

import com.snowplowanalytics.snowplow.streams.http.HttpSinkConfig

object HttpSink {

  def resource[F[_]: Async](config: HttpSinkConfig): Resource[F, Sink[F]] =
    createHttpClient
      .map { httpClient =>
        new Sink[F] {
          override def sink(batch: ListOfList[Sinkable]): F[Unit] =
            batch.parTraverse_(sendRequest(config, httpClient, _))

          override def isHealthy: F[Boolean] = Async[F].pure(true)
        }
      }

  private def sendRequest[F[_]: Async](
    config: HttpSinkConfig,
    httpClient: Client[F],
    message: Sinkable
  ): F[Unit] = {
    val endpoint = config.baseUri / "input"
    val request = Request[F](Method.POST, endpoint)
      .withContentType(`Content-Type`(MediaType.application.`octet-stream`))
      .withEntity(message.bytes)
    httpClient
      .run(request)
      .use { response =>
        if (response.status.isSuccess) Async[F].unit
        else
          response
            .as[String]
            .flatMap(body => Async[F].raiseError(new Exception(s"non-2xx response: \n$body")))
      }
  }

  private def createHttpClient[F[_]: Async]: Resource[F, Client[F]] =
    BlazeClientBuilder[F]
      .withMaxTotalConnections(Int.MaxValue)
      .withMaxWaitQueueLimit(Int.MaxValue)
      .resource
}
