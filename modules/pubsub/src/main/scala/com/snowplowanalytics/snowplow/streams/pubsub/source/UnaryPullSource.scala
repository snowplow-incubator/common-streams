/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.{Async, Ref, Sync}
import cats.effect.kernel.Unique
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// pubsub
import com.google.pubsub.v1.{PullRequest, PullResponse, ReceivedMessage}
import com.google.cloud.pubsub.v1.stub.SubscriberStub

// snowplow
import com.snowplowanalytics.snowplow.streams.internal.LowLevelEvents
import com.snowplowanalytics.snowplow.streams.pubsub.{FutureInterop, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps.implicits._

import scala.jdk.CollectionConverters._

/**
 * An fs2 Stream based around a GRPC unary pull. It is the source's fetch loop, which `PubsubSource`
 * runs in the background and hands over to downstream.
 *
 * Unary pull is the better choice for apps that build up large numbers of un-acked messages, e.g.
 * Lake Loader.
 */
private[source] object UnaryPullSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  /**
   * Each pull first reads the state, inside the function `parEvalMapUnordered` runs, because a tick
   * can wait for a free slot after it is taken, so a check on the tick itself could be stale. Once
   * the state is released, no new pull starts: the function returns `None` instead, and the loop
   * blocks offering it to the handoff. If the gate is still running and downstream pulls from it
   * again, the gate takes that `None` and ends; otherwise the offer blocks until the loop is
   * cancelled at the end of shutdown.
   *
   * A pull that passed the check just before the release is in flight at the release. It completes,
   * and its batch is rejected and nacked. Until it completes, it can still retry on a transient
   * gRPC error, up to `transientErrors.attempts`, and each retry is a new RPC that can start after
   * the release. This is accepted: such retries are rare, and their batches are nacked like any
   * other in-flight pull's, so they cost only latency.
   *
   * @param onFailure
   *   Called with the error of a pull that fails, before the stream raises it, unless the loop is
   *   already being cancelled after an earlier failure. With parallel pulls, `parEvalMapUnordered`
   *   raises a pull's error only when the stream is next pulled, which never happens if downstream
   *   is blocked handing over an earlier batch. When parallel pulls fail close together, it cancels
   *   the later ones, and because a pull is uncancelable with `onError` outside it, the cancel
   *   lands before `onError` runs. So only the first failure is reported, which is enough: it is
   *   logged and fails the stream.
   */
  def stream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState],
    onFailure: Throwable => F[Unit]
  ): Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]] = {
    val parallelPullCount = chooseNumParallelPulls(config)
    Stream
      .fixedRateStartImmediately(config.debounceRequests, dampen = true)
      .parEvalMapUnordered(parallelPullCount) { _ =>
        refStates.get.flatMap { state =>
          if (state.isReleased)
            Logger[F]
              .debug("Un-emitted pubsub messages were released, so not starting a pull")
              .as(none[LowLevelEvents[Vector[Unique.Token]]])
          else
            pullAndManageState(config, stub, refStates).onError { case e => onFailure(e) }
        }
      }
  }

  private def pullAndManageState[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): F[Option[LowLevelEvents[Vector[Unique.Token]]]] =
    Async[F].uncancelable { poll =>
      for {
        _ <- Logger[F].debug("Pulling from subscription")
        records <- pullFromSubscription(config, stub).retryingOnTransientGrpcFailuresWithCancelableSleep(
                     config.retries.transientErrors.delay,
                     config.retries.transientErrors.attempts,
                     poll
                   )
        _ <- Logger[F].debug(s"Pulled ${records.size} messages")
        result <- PubsubSource.handleReceivedMessages(config, stub, refStates, true, records)
      } yield result
    }

  /**
   * Wrapper around the "Pull" PubSub GRPC.
   *
   * @return
   *   The PullResponse, comprising a batch of pubsub messages
   */
  private def pullFromSubscription[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub
  ): F[Vector[ReceivedMessage]] = {
    val request = PullRequest.newBuilder
      .setSubscription(config.subscription.show)
      .setMaxMessages(config.maxMessagesPerPull)
      .build
    for {
      apiFuture <- Sync[F].delay(stub.pullCallable.futureCall(request))
      res <- FutureInterop.fromFuture[F, PullResponse](apiFuture)
    } yield res.getReceivedMessagesList.asScala.toVector
  }

  /**
   * Converts `parallelPullFactor` to a suggested number of parallel pulls
   *
   * For bigger instances (more cores) the downstream processor can typically process events more
   * quickly. So the PubSub subscriber needs more parallelism in order to keep downstream saturated
   * with events.
   */
  private def chooseNumParallelPulls(config: PubsubSourceConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.parallelPullFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

}
