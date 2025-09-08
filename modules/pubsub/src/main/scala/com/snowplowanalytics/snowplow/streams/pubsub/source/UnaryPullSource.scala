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
import fs2.{Pipe, Stream}
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
 * And fs2 Stream based around a GRPC unary pull
 *
 * Unary pull is the better choice for apps that build up large numbers of un-acked messages, e.g.
 * Lake Loader.
 */
private[source] object UnaryPullSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]] = {
    val parallelPullCount = chooseNumParallelPulls(config)
    Stream
      .fixedRateStartImmediately(config.debounceRequests, dampen = true)
      .through(parEvalMapUnorderedOrPrefetch(parallelPullCount)(_ => pullAndManageState(config, stub, refStates)))
  }

  private def parEvalMapUnorderedOrPrefetch[F[_]: Async, A, B](parallelPullCount: Int)(f: A => F[B]): Pipe[F, A, B] =
    if (parallelPullCount == 1)
      // If parallelPullCount is 1, we need a prefetch so the behaviour of this source is roughly
      // consistent with other sources and other values of parallePullCount
      _.evalMap(f).prefetch
    else
      // If parallelPullCount > 1, then we don't need prefetch, because parEvalMapUnordered already
      // has the effect of pre-fetching
      _.parEvalMapUnordered(parallelPullCount)(f)

  private def pullAndManageState[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
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
