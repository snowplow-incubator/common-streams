/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import com.google.api.core.{ApiFuture, ApiFutures, SettableApiFuture}
import com.google.api.gax.rpc.{
  ApiCallContext,
  BidiStreamingCallable,
  ClientStream,
  ClientStreamReadyObserver,
  ResponseObserver,
  StreamController,
  UnaryCallable
}
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.{ByteString, Empty, Timestamp}
import com.google.pubsub.v1.{
  AcknowledgeRequest,
  ModifyAckDeadlineRequest,
  PubsubMessage,
  PullRequest,
  PullResponse,
  ReceivedMessage,
  StreamingPullRequest,
  StreamingPullResponse
}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.jdk.CollectionConverters._

/**
 * A `SubscriberStub` that needs no pubsub server.
 *
 * Every pull returns one message, whose ack ID is also its data, until `messageLimit` messages have
 * been returned: after that, a unary pull returns no messages. A streaming pull sends one message
 * for each message the source requests. Every modack is recorded, and a nack (a modack with zero
 * duration) raises an error if `failNacks` is set. Every ack is recorded.
 *
 * Setting `failPulls` makes every later pull fail, and setting `failExtensions` makes every later
 * modack with a non-zero duration fail.
 *
 * Setting `holdPulls` makes every later unary pull stay open, without a response, until
 * `completeHeldPulls` is called.
 */
class FakeSubscriberStub(failNacks: Boolean, messageLimit: Int = Int.MaxValue) extends SubscriberStub {

  val failPulls: AtomicBoolean      = new AtomicBoolean(false)
  val failExtensions: AtomicBoolean = new AtomicBoolean(false)
  val holdPulls: AtomicBoolean      = new AtomicBoolean(false)

  private val pullCount    = new AtomicInteger(0)
  private val messageCount = new AtomicInteger(0)
  private val modAcks      = new AtomicReference(Vector.empty[(Vector[String], Int)])
  private val failedExts   = new AtomicInteger(0)
  private val heldPulls    = new AtomicReference(Vector.empty[SettableApiFuture[PullResponse]])

  private val ackedIds = new AtomicReference(Vector.empty[String])

  /** Unary pulls, or messages requested on a streaming pull */
  def pulls: Int = pullCount.get

  def acked: Vector[String] = ackedIds.get

  /** Every ack ID that was nacked, including attempts that raised an error */
  def nacked: Vector[String] =
    modAcks.get.collect { case (ackIds, 0) => ackIds }.flatten

  /**
   * Every ack ID whose deadline was extended, once per extension, including attempts that failed
   */
  def extended: Vector[String] =
    modAcks.get.collect { case (ackIds, s) if s > 0 => ackIds }.flatten

  /** Extensions that raised an error because `failExtensions` was set */
  def failedExtensions: Int = failedExts.get

  /** Unary pulls held open by `holdPulls` that have not been completed yet */
  def held: Int = heldPulls.get.size

  /**
   * Stops holding later pulls, and completes every held pull, each with the next message
   */
  def completeHeldPulls(): Unit = {
    holdPulls.set(false)
    heldPulls.getAndSet(Vector.empty).foreach(_.set(pullResponse()))
  }

  private def pullResponse(): PullResponse =
    PullResponse.newBuilder.addAllReceivedMessages(nextMessage().toList.asJava).build

  private def nextMessage(): Option[ReceivedMessage] = {
    val n = messageCount.incrementAndGet()
    if (n <= messageLimit) Some(FakeSubscriberStub.message(s"ack-$n")) else None
  }

  override def pullCallable: UnaryCallable[PullRequest, PullResponse] =
    new UnaryCallable[PullRequest, PullResponse] {
      override def futureCall(request: PullRequest, context: ApiCallContext): ApiFuture[PullResponse] = {
        pullCount.incrementAndGet()
        if (failPulls.get)
          ApiFutures.immediateFailedFuture(new RuntimeException("boom! pull failed"))
        else if (holdPulls.get) {
          val future = SettableApiFuture.create[PullResponse]()
          heldPulls.updateAndGet(_ :+ future)
          future
        } else
          ApiFutures.immediateFuture(pullResponse())
      }
    }

  override def streamingPullCallable: BidiStreamingCallable[StreamingPullRequest, StreamingPullResponse] =
    new BidiStreamingCallable[StreamingPullRequest, StreamingPullResponse] {
      override def internalCall(
        observer: ResponseObserver[StreamingPullResponse],
        ready: ClientStreamReadyObserver[StreamingPullRequest],
        context: ApiCallContext
      ): ClientStream[StreamingPullRequest] = {
        observer.onStart(new StreamController {
          override def cancel(): Unit                        = ()
          override def disableAutoInboundFlowControl(): Unit = ()
          override def request(count: Int): Unit =
            (1 to count).foreach { _ =>
              pullCount.incrementAndGet()
              if (failPulls.get)
                observer.onError(new RuntimeException("boom! pull failed"))
              else
                nextMessage().foreach { m =>
                  observer.onResponse(StreamingPullResponse.newBuilder.addReceivedMessages(m).build)
                }
            }
        })
        new ClientStream[StreamingPullRequest] {
          override def send(request: StreamingPullRequest): Unit = ()
          override def closeSendWithError(t: Throwable): Unit    = ()
          override def closeSend(): Unit                         = ()
          override def isSendReady: Boolean                      = true
        }
      }
    }

  override def acknowledgeCallable: UnaryCallable[AcknowledgeRequest, Empty] =
    new UnaryCallable[AcknowledgeRequest, Empty] {
      override def futureCall(request: AcknowledgeRequest, context: ApiCallContext): ApiFuture[Empty] = {
        ackedIds.updateAndGet(_ ++ request.getAckIdsList.asScala)
        ApiFutures.immediateFuture(Empty.getDefaultInstance)
      }
    }

  override def modifyAckDeadlineCallable: UnaryCallable[ModifyAckDeadlineRequest, Empty] =
    new UnaryCallable[ModifyAckDeadlineRequest, Empty] {
      override def futureCall(request: ModifyAckDeadlineRequest, context: ApiCallContext): ApiFuture[Empty] = {
        val seconds = request.getAckDeadlineSeconds
        modAcks.updateAndGet(_ :+ (request.getAckIdsList.asScala.toVector -> seconds))
        if (failNacks && seconds == 0)
          ApiFutures.immediateFailedFuture(new RuntimeException("boom! nack failed"))
        else if (failExtensions.get && seconds > 0) {
          failedExts.incrementAndGet()
          ApiFutures.immediateFailedFuture(new RuntimeException("boom! extension failed"))
        } else
          ApiFutures.immediateFuture(Empty.getDefaultInstance)
      }
    }

  override def close(): Unit                                             = ()
  override def shutdown(): Unit                                          = ()
  override def isShutdown: Boolean                                       = false
  override def isTerminated: Boolean                                     = false
  override def shutdownNow(): Unit                                       = ()
  override def awaitTermination(duration: Long, unit: TimeUnit): Boolean = true
}

object FakeSubscriberStub {
  def message(ackId: String): ReceivedMessage =
    ReceivedMessage.newBuilder
      .setAckId(ackId)
      .setMessage(
        PubsubMessage.newBuilder
          .setMessageId(ackId)
          .setData(ByteString.copyFromUtf8(ackId))
          .setPublishTime(Timestamp.newBuilder.setSeconds(1L))
      )
      .build
}
