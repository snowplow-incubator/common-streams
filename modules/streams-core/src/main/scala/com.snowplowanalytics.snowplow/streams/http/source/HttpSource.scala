/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.http.source

import cats.Applicative
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.Queue
import cats.implicits._

import fs2.{Chunk, Stream}

import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.blaze.server.BlazeServerBuilder

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scodec.bits.ByteVector

import java.nio.ByteBuffer

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.http.HttpSourceConfig
import com.snowplowanalytics.snowplow.streams.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

object HttpSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /**
   * Builds a SourceAndAck that receives payloads over HTTP
   *
   * POST requests to /input with Content-Type: application/octet-stream are accepted. Each request
   * body is passed to the application as a single ByteBuffer.
   *
   * @param config
   *   Configuration for the HTTP source
   * @return
   *   A Resource containing the SourceAndAck
   */
  def resource[F[_]: Async](config: HttpSourceConfig): Resource[F, SourceAndAck[F]] =
    for {
      queue <- Resource.eval(Queue.unbounded[F, ByteBuffer])
      _ <- startServer(config, queue)
      sourceAndAck <- Resource.eval(LowLevelSource.toSourceAndAck(lowLevel(queue)))
    } yield sourceAndAck

  private def lowLevel[F[_]: Async](queue: Queue[F, ByteBuffer]): LowLevelSource[F, Unit] =
    new LowLevelSource[F, Unit] {
      def checkpointer: Checkpointer[F, Unit] = noOpCheckpointer

      def stream: Stream[F, Stream[F, Option[LowLevelEvents[Unit]]]] =
        httpStream(queue)

      def debounceCheckpoints: FiniteDuration = Duration.Zero
    }

  private def noOpCheckpointer[F[_]: Applicative]: Checkpointer[F, Unit] =
    new Checkpointer[F, Unit] {
      def combine(x: Unit, y: Unit): Unit = ()
      val empty: Unit = ()
      def ack(c: Unit): F[Unit]  = Applicative[F].unit
      def nack(c: Unit): F[Unit] = Applicative[F].unit
    }

  private def httpStream[F[_]: Async](
    queue: Queue[F, ByteBuffer]
  ): Stream[F, Stream[F, Option[LowLevelEvents[Unit]]]] =
    Stream.emit {
      Stream
        .fromQueueUnterminated(queue)
        .map { buffer =>
          Some(LowLevelEvents(Chunk.singleton(buffer), (), None))
        }
        .mergeHaltL(Stream.awakeDelay(10.seconds).map(_ => None).repeat) // keepalives
    }

  private def startServer[F[_]: Async](
    config: HttpSourceConfig,
    queue: Queue[F, ByteBuffer]
  ): Resource[F, Unit] =
    BlazeServerBuilder[F]
      .bindHttp(config.port.value, "0.0.0.0")
      .withHttpApp(routes(queue).orNotFound)
      .resource
      .evalTap(_ => Logger[F].info(s"HTTP source server started on port ${config.port}"))
      .onFinalize(Logger[F].info(s"HTTP source server stopped on port ${config.port}"))
      .void

  private def routes[F[_]: Async](queue: Queue[F, ByteBuffer]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    HttpRoutes.of[F] { case req @ POST -> Root / "input" =>
      req.contentType match {
        case Some(ct) if ct.mediaType == MediaType.application.`octet-stream` =>
          for {
            bytes <- req.body.compile.to(ByteVector)
            _ <- queue.offer(bytes.toByteBufferUnsafe)
            response <- Ok()
          } yield response
        case _ =>
          UnsupportedMediaType("Content-Type must be application/octet-stream")
      }
    }
  }
}
