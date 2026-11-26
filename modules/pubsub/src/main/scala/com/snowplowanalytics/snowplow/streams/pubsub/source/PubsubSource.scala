/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.effect.implicits._
import cats.effect.kernel.Unique
import cats.effect.std.{Queue, QueueSource}
import cats.implicits._
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// pubsub
import com.google.api.gax.core.{CredentialsProvider, FixedExecutorProvider}
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.pubsub.v1.ReceivedMessage
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub, SubscriberStubSettings}

// snowplow
import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.pubsub.PubsubSourceConfig

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}

/**
 * A common-streams `Source` that pulls messages from PubSub
 *
 * This Source is a wrapper around a GRPC stub. It can use either "Unary Pull" or "Streaming Pull"
 * GRPC methods to fetch events, based on the `streamingPull` configuration parameter.
 */
private[pubsub] object PubsubSource {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](
    config: PubsubSourceConfig,
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider,
    credentials: CredentialsProvider
  ): Resource[F, SourceAndAck[F]] =
    for {
      ref <- Resource.eval(Ref[F].of(PubsubSourceState.initial))
      stub <- buildSubscriberStub(transport, executor, credentials)
      source <- Resource.eval(LowLevelSource.toSourceAndAck(lowLevel(config, stub, ref)))
    } yield source

  private[source] def lowLevel[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): LowLevelSource[F, Vector[Unique.Token]] =
    new LowLevelSource[F, Vector[Unique.Token]] {
      override def checkpointer: Checkpointer[F, Vector[Unique.Token]] = new PubsubCheckpointer(config, stub, refStates)

      override def stream: Stream[F, Stream[F, Option[LowLevelEvents[Vector[Unique.Token]]]]] =
        pubsubStream(config, stub, refStates)

      override def debounceCheckpoints: FiniteDuration = Duration.Zero

      override def releaseUnemitted: F[Unit] =
        PubsubSource.releaseUnemitted(config, stub, refStates)
    }

  private type Item = Option[LowLevelEvents[Vector[Unique.Token]]]

  /**
   * The outer stream holds the leases: the ack IDs of every batch in the state, which
   * `extendDeadlines` extends until each is acked or nacked, and which are all nacked when the
   * outer stream is finalized. It also runs the fetch loop. Both run as backgrounds of the outer
   * stream, so they outlive an inner stream that ends or fails while windows are still finishing.
   *
   * It emits a single inner stream, the [[gate]], which hands the fetch loop's output downstream
   * through a synchronous queue. With unary pull, the gate ends at the first item it takes after
   * un-emitted batches are released. With streaming pull nothing is released, so the gate never
   * ends by itself. Either way, the outer stream stays open until it is interrupted at the end of
   * shutdown, and the gate with it.
   *
   * Resources are released in reverse order, so both backgrounds have stopped before the final nack
   * reads the state.
   *
   * Each background logs its own failure where it happens, saying what it means for the batches,
   * and completes `failed` with the error. The gate raises that error while the state is pulling.
   * An error that also fails the stream is therefore logged twice, which is accepted so that no
   * error is lost: one that happens after the release, or while the fetch loop is blocked offering
   * a batch the gate never takes, is still logged.
   *
   * A background's error reaches the stream only when downstream next pulls from the inner stream.
   * This is a deliberate trade-off that keeps the backgrounds independent of the inner stream: a
   * failed fetch does not stop deadline extension while windows finish.
   */
  private def pubsubStream[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): Stream[F, Stream[F, Item]] =
    for {
      _ <- Stream.resource(Resource.onFinalize(nackRefStatesForShutdown(config, stub, refStates)))
      handoff <- Stream.eval(Queue.synchronous[F, Item])
      failed <- Stream.eval(Deferred[F, Throwable])
      fetchFailed = reportFailure(
                      failed,
                      "Error fetching from pubsub. Fetching has stopped. Batches already handed downstream are unaffected."
                    ) _
      fetchLoop = if (config.streamingPull)
                    StreamingPullSource.stream(config, stub, refStates, fetchFailed)
                  else
                    UnaryPullSource.stream(config, stub, refStates, fetchFailed)
      // The fetch loop has already reported any error that stops it, so the background's outcome is not needed
      _ <- Stream.resource(fetchLoop.evalMap(handoff.offer).compile.drain.background)
      _ <- Stream.resource {
             extendDeadlines(config, stub, refStates).compile.drain
               .handleErrorWith(
                 reportFailure(
                   failed,
                   "Error extending ack deadlines. Deadline extension has stopped, so pubsub may redeliver batches that are still being processed."
                 )
               )
               .background
           }
      inner <- holdOpenAfter(Stream.emit(gate(handoff, refStates, failed)).covary[F])
    } yield inner

  /**
   * Keeps the outer stream, and with it the leases (the final nack and deadline extension), open
   * after the given stream ends, until the outer stream is interrupted at the end of shutdown. An
   * outer stream that ends is finalized at once, which would nack batches the windows are still
   * processing.
   */
  private def holdOpenAfter[F[_]: Async, A](stream: Stream[F, A]): Stream[F, A] =
    stream ++ Stream.never[F]

  private def reportFailure[F[_]: Sync](failed: Deferred[F, Throwable], message: String)(e: Throwable): F[Unit] =
    Logger[F].error(e)(message) >> failed.complete(e).void

  /**
   * The inner stream. Each item from the handoff is handed downstream only if the state is still
   * pulling, decided in the same `Ref.modify` that moves its batches from buffered to emitted. Once
   * the state is released, the next item of any kind, `None` included, ends the stream. With unary
   * pull, the fetch loop starts no new pull after the release: only pulls in flight at the release,
   * at most one per parallel pull, still return batches, which the state rejects and are nacked. A
   * pull in flight at the release can still retry on a transient error, as described in
   * `UnaryPullSource.stream`.
   *
   * Batches handed over but still waiting in common-streams' generic windowing are not released
   * early: they are nacked at the end of shutdown. This is a trade-off that keeps per-batch work
   * off the generic hot path.
   *
   * Once `failed` is completed, the gate raises its error while the state is pulling, and ends once
   * the state is released, because the background that failed has already logged it. If the fetch
   * loop is blocked offering an item when `failed` is completed, that item is usually dropped, as
   * is one taken in the same instant. Its batches are still buffered in the state, so they are
   * released or nacked at the end of shutdown like any other.
   */
  private[source] def gate[F[_]: Async](
    handoff: QueueSource[F, Item],
    refStates: Ref[F, PubsubSourceState],
    failed: Deferred[F, Throwable]
  ): Stream[F, Item] =
    Stream
      .repeatEval(Async[F].race(failed.get, handoff.take))
      .evalMap {
        case Right(item) =>
          refStates.modify(_.handOver(item.fold(Vector.empty[Unique.Token])(_.ack))).flatMap { pulling =>
            if (pulling) Sync[F].pure(Option(item))
            else Logger[F].debug("Un-emitted pubsub messages were released, so ending the source's stream").as(Option.empty[Item])
          }
        case Left(error) =>
          refStates.get.flatMap { state =>
            if (state.isReleased) Sync[F].pure(Option.empty[Item])
            else Sync[F].raiseError[Option[Item]](error)
          }
      }
      .unNoneTerminate

  /**
   * Function to be called immediately after receiving pubsub message so we can start managing state
   *
   * Managing state of the batch includes:
   *
   *   - Extend the ack deadline, which gives us some time to process this batch.
   *   - Generate a unique token by which to identify this batch internally
   *   - Add the batch to the local "State" so that we can re-extend the ack deadline if needed
   */
  private[source] def handleReceivedMessages[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState],
    modAckImmediately: Boolean,
    records: Vector[ReceivedMessage]
  ): F[Option[LowLevelEvents[Vector[Unique.Token]]]] =
    if (records.nonEmpty) {
      val chunk  = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
      val ackIds = records.map(_.getAckId)
      for {
        _ <- Logger[F].trace {
               records.map(_.getMessage.getMessageId).mkString("Pubsub message IDs: ", ",", "")
             }
        timeReceived <- Sync[F].realTimeInstant
        _ <- if (modAckImmediately) Utils.modAck[F](config, stub, ackIds, config.durationPerAckExtension) else Sync[F].unit
        token <- Unique[F].unique
        currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
        accepted <- refStates.modify(_.receive(token, PubsubBatchState(currentDeadline, ackIds)))
        _ <- if (accepted) Sync[F].unit
             else nackRejected(config, stub, ackIds)
      } yield Some(LowLevelEvents(chunk, Vector(token), Some(earliestTimestampOfRecords(records))))
    } else {
      none.pure[F]
    }

  /**
   * Nacks a batch that the state rejected because un-emitted batches were already released.
   *
   * Errors are logged and not raised. This runs only after the release, when a raised error could
   * no longer fail the stream, and would stop the fetch loop for nothing. A batch whose nack fails
   * is redelivered by pubsub when its ack deadline expires, which costs latency only.
   */
  private def nackRejected[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    ackIds: Vector[String]
  ): F[Unit] =
    Logger[F].debug(s"Nacking ${ackIds.size} messages received after un-emitted messages were released") >>
      Utils.modAck[F](config, stub, ackIds, Duration.Zero).handleErrorWith { e =>
        Logger[F].warn(e)(
          s"Error nacking ${ackIds.size} messages received after un-emitted messages were released. Pubsub will redeliver them when their ack deadline expires"
        )
      }

  private def earliestTimestampOfRecords(records: Vector[ReceivedMessage]): FiniteDuration = {
    val (tstampSeconds, tstampNanos) =
      records.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
    tstampSeconds.seconds + tstampNanos.toLong.nanos
  }

  /**
   * "Nacks" every batch still in the state: batches still buffered in the source, and emitted
   * batches that the app has not acked yet.
   *
   * This is called when the outer stream is finalized, at the end of shutdown or after the outer
   * stream ends for any other reason. It allows PubSub to immediately re-deliver the messages to a
   * different pod; instead of waiting for the ack deadline to expire.
   */
  private def nackRefStatesForShutdown[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): F[Unit] =
    refStates.modify(_.removeAll).flatMap { batches =>
      Utils.modAck(config, stub, batches.flatMap(_.ackIds), Duration.Zero)
    }

  /**
   * With unary pull, "nacks" every message that was pulled from pubsub but not yet emitted
   * downstream, and moves the state to `Released`, which ends the inner stream at its next item,
   * stops the fetch loop starting new pulls (a pull already in flight can still retry on a
   * transient error), and rejects every batch received afterwards.
   *
   * This is called at the start of graceful shutdown, so pubsub can re-deliver these messages to a
   * different pod while this app is still finalizing the batches it has already emitted.
   *
   * The batches leave the state before they are nacked. If the nack fails, its error is raised (and
   * logged by common-streams), and the batches are not nacked again at the end of shutdown: pubsub
   * redelivers them when their ack deadline expires. That costs latency only, and is accepted to
   * keep the release a single atomic step.
   *
   * Streaming pull does not release early. In a test against a real subscription, messages it
   * released reached other consumers later than without the release, by a delay that scaled with
   * the ack extension. They appear to go back to this pod's still-open stream and wait there for a
   * new ack deadline, but that cause is inferred, not observed.
   */
  private[source] def releaseUnemitted[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): F[Unit] =
    if (config.streamingPull)
      Sync[F].unit
    else
      refStates.modify(_.release).flatMap { batches =>
        val ackIds = batches.flatMap(_.ackIds)
        if (ackIds.isEmpty)
          Logger[F].debug("No un-emitted pubsub messages to release")
        else
          Logger[F].info(s"Releasing ${ackIds.size} un-emitted messages back to pubsub") >>
            Utils.modAck(config, stub, ackIds, Duration.Zero)
      }

  /**
   * Modify ack deadlines if we need more time to process the messages
   *
   * @param config
   *   The Source configuration
   * @param stub
   *   The GRPC stub on which we can issue modack requests
   * @param refStates
   *   The source's state, holding the data about each batch of messages received from pubsub,
   *   buffered or emitted. This function must update the state if it extends a deadline. A batch
   *   released or removed between this function reading the state and sending its modack is
   *   extended anyway, which delays its redelivery but is otherwise harmless.
   */
  private def extendDeadlines[F[_]: Async](
    config: PubsubSourceConfig,
    stub: SubscriberStub,
    refStates: Ref[F, PubsubSourceState]
  ): Stream[F, Nothing] =
    Stream
      .eval(Sync[F].realTimeInstant)
      .evalMap { now =>
        val minAllowedDeadline = now.plusMillis((config.minRemainingAckDeadline.toDouble * config.durationPerAckExtension.toMillis).toLong)
        val newDeadline        = now.plusMillis(config.durationPerAckExtension.toMillis)
        refStates.modify(_.extendDeadlines(minAllowedDeadline, newDeadline))
      }
      .evalMap { toExtend =>
        if (toExtend.isEmpty)
          // If no message had a deadline close to expiry, then sleep for an appropriate amount of time and check again
          Sync[F].sleep(0.5 * config.minRemainingAckDeadline.toDouble * config.durationPerAckExtension)
        else {
          val ackIds = toExtend.sortBy(_.currentDeadline).flatMap(_.ackIds)
          Utils.modAck[F](config, stub, ackIds, config.durationPerAckExtension)
        }
      }
      .repeat
      .drain

  /**
   * Builds the "Stub" which is the object from which we can call PubSub SDK methods
   *
   * This implementation has some hard-coded values, which have been copied over from the equivalent
   * hard-coded values in the java-pubsub client library.
   */
  private def buildSubscriberStub[F[_]: Sync](
    transport: FixedTransportChannelProvider,
    executor: FixedExecutorProvider,
    credentials: CredentialsProvider
  ): Resource[F, GrpcSubscriberStub] = {
    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(executor)
      .setTransportChannelProvider(transport)
      .setCredentialsProvider(credentials)
      .build

    Resource.make(Sync[F].delay(GrpcSubscriberStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

}
