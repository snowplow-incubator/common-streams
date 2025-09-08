/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.effect.std.{Dispatcher, Queue, QueueSink, QueueSource}
import cats.effect.kernel.Unique
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// pubsub
import com.google.api.gax.rpc.{ResponseObserver, StreamController}
import com.google.pubsub.v1.{ReceivedMessage, StreamingPullRequest, StreamingPullResponse}
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.grpc.Status

// snowplow
import com.snowplowanalytics.snowplow.streams.internal.LowLevelEvents
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubSourceConfig
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubRetryOps

import java.util.UUID
import scala.jdk.CollectionConverters._

/**
 * An fs2 stream based around a GRPC streaming pull
 *
 * Streaming pull is the better choice for apps with low latency requirements.
 */
private[source] object StreamingPullSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]] =
    Stream.eval(Sync[F].delay(UUID.randomUUID)).flatMap { clientId =>
      Stream
        .resource(startStreamingPull(config, clientId, stub))
        .flatMap { case (controller, queue) =>
          Stream
            .eval(pullAndManageState(config, stub, controller, queue, refStates))
            .repeat
            .collectWhile { case Right(ok) => ok }
            .cons1(None)
        }
        .repeat
        .prefetch
    }

  /**
   * Actions passed from the ResponseObserver callbacks to the main processing logic via a Queue.
   */
  private sealed trait SubscriberAction
  private object SubscriberAction {

    /** Messages received from PubSub to be processed */
    case class ProcessRecords(records: Vector[ReceivedMessage]) extends SubscriberAction

    /** Error from the ResponseObserver that needs to be handled */
    case class Error(t: Throwable) extends SubscriberAction
  }

  /**
   * Pulls one action from the queue and processes it.
   *
   * @return
   *   Left when the streaming pull should be terminated, but an attempt should be made to open a
   *   new streaming pull (i.e. retryable error). Right when records were processed and streaming
   *   pull should continue. Raises an exception in `F` if an error should not be retried
   */
  private def pullAndManageState[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    controller: StreamController,
    queue: QueueSource[F, SubscriberAction],
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): F[Either[Unit, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
    Sync[F].uncancelable { _ =>
      for {
        _ <- Logger[F].debug("Requesting from controller")
        _ <- Sync[F].delay(controller.request(1))
        action <- queue.take
        either <- action match {
                    case SubscriberAction.ProcessRecords(records) =>
                      for {
                        _ <- Logger[F].debug(s"Pulled ${records.size} messages")
                        result <- PubsubSource.handleReceivedMessages(config, stub, refStates, false, records)
                      } yield Right(result)
                    case SubscriberAction.Error(t) if PubsubRetryOps.isRetryableException(t) =>
                      // Log at debug level because retryable errors are very frequent.
                      // In particular, if the pubsub subscription is empty then a streaming pull returns UNAVAILABLE
                      for {
                        _ <- Logger[F].debug(t)("Retryable error on PubSub streaming pull")
                        _ <- Async[F].sleep(config.retries.transientErrors.delay)
                      } yield Left(())
                    case SubscriberAction.Error(t) =>
                      Logger[F].error(t)("Exception from PubSub streaming pull") >> Sync[F].raiseError(t)
                  }
      } yield either
    }

  /**
   * Wrapper around the "StreamingPull" PubSub GRPC.
   *
   * @return
   *   A `Resource` comprising the grpc `StreamController` which may be used request more messages,
   *   and a `Queue` on which the messages will be passed.
   */
  private def startStreamingPull[F[_]: Async](
    config: PubsubSourceConfig,
    clientId: UUID,
    stub: SubscriberStub
  ): Resource[F, (StreamController, QueueSource[F, SubscriberAction])] = {
    val request = StreamingPullRequest.newBuilder
      .setSubscription(config.subscription.show)
      .setStreamAckDeadlineSeconds(config.durationPerAckExtension.toSeconds.toInt)
      .setMaxOutstandingMessages(0)
      .setMaxOutstandingBytes(0)
      .setClientId(clientId.toString)
      .build

    for {
      dispatcher <- Dispatcher.sequential(false)
      deferred <- Resource.eval(Deferred[F, StreamController])
      queue <- Resource.eval(Queue.synchronous[F, SubscriberAction])
      o = observer(dispatcher, deferred, queue)
      stream <- Resource.make {
                  Sync[F].delay(stub.streamingPullCallable.splitCall(o))
                } { stream =>
                  Sync[F].delay(stream.closeSendWithError(Status.CANCELLED.asException))
                }
      _ <- Resource.eval(Sync[F].delay(stream.send(request)))
      controller <- Resource.eval(deferred.get)
    } yield (controller, queue)
  }

  private def observer[F[_]](
    dispatcher: Dispatcher[F],
    deferred: Deferred[F, StreamController],
    queue: QueueSink[F, SubscriberAction]
  ): ResponseObserver[StreamingPullResponse] =
    new ResponseObserver[StreamingPullResponse] {
      override def onResponse(response: StreamingPullResponse): Unit =
        dispatcher.unsafeRunAndForget {
          queue.offer(SubscriberAction.ProcessRecords(response.getReceivedMessagesList.asScala.toVector))
        }

      override def onStart(c: StreamController): Unit = {
        c.disableAutoInboundFlowControl()
        dispatcher.unsafeRunAndForget {
          deferred.complete(c)
        }
      }

      override def onError(t: Throwable): Unit =
        dispatcher.unsafeRunAndForget {
          queue.offer(SubscriberAction.Error(t))
        }

      override def onComplete(): Unit = ()

    }

}
