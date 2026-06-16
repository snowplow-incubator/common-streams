/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.compression

import java.nio.ByteBuffer
import java.time.Instant

import cats.effect.kernel.Unique
import cats.effect.Sync

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, SourceAndAck}

object Decompression {

  /**
   * An application that processes a source of decompressed events
   *
   * Use this instead of [[EventProcessor]] when calling `decompressedStream` on a [[SourceAndAck]].
   * The source handles decompression before handing batches to the processor, so the processor
   * receives already-decompressed payloads along with any bad rows produced during decompression.
   *
   * The [[DecompressedEventProcessor]] must emit the `ack` token from [[DecompressedTokenedEvents]]
   * after fully processing each group. Note that a single source message may yield multiple batches
   * with `ack = None` followed by a final batch with `Some(token)` — the token must be emitted only
   * once, after processing the final batch.
   */
  type DecompressedEventProcessor[F[_]] = Pipe[F, DecompressedTokenedEvents, Unique.Token]

  /**
   * A batch of de-compressed payloads as fed into a [[DecompressedEventProcessor]]
   *
   * `decompressedStream` on [[SourceAndAck]] handles decompression before handing batches to the
   * [[DecompressedEventProcessor]], so the processor always receives already-decompressed payloads.
   *
   * A single incoming stream message (i.e. [[TokenedEvents]]) might yield more than one
   * [[DecompressedTokenedEvents]] if the decompressed size exceeds the configured batch size limit.
   * In that case only the final [[DecompressedTokenedEvents]] carries an [[ack]] token; earlier
   * ones have [[ack]] set to `None`. The [[DecompressedEventProcessor]] must emit the token once it
   * has processed all events from the group.
   *
   * @param payloads
   *   The de-compressed payloads in this batch.
   * @param bad
   *   Bad rows produced while decompressing the source messages (e.g. corrupt or oversized
   *   payloads).
   * @param ack
   *   The token to emit after fully processing this batch (and all preceding batches from the same
   *   source message). `None` for intermediate over-size batches; `Some` for the final batch.
   * @param payloadVersion
   *   The payload format version from the compression header. `None` for uncompressed records.
   */
  case class DecompressedTokenedEvents(
    payloads: List[ByteBuffer],
    bad: List[BadRow],
    ack: Option[Unique.Token],
    payloadVersion: Option[Int]
  )

  implicit class SourceAndAckOps[F[_]: Sync](underlying: SourceAndAck[F]) {

    /**
     * Like [[SourceAndAck.stream]], but applies decompression before handing batches to the
     * processor.
     *
     * Decompression errors are surfaced as bad rows inside [[DecompressedTokenedEvents]].
     *
     * @param config
     *   Configures how events are fed into the processor, e.g. whether to use timed windows
     * @param decompressionConfig
     *   Controls batch-size and per-payload size limits during decompression
     * @param processor
     *   The [[DecompressedEventProcessor]] implemented by the application
     * @param badRowProcessor
     *   Metadata about the application, used when creating size violation bad rows
     * @param toBadRow
     *   Three failure modes are distinguished:
     *   - [[Decompressor.RecordTooBig]] — a single record would exceed
     *     `decompressionConfig.maxBytesSinglePayload` after decompression. The record is skipped
     *     (the decompressor stays functional) and a `BadRow.SizeViolation` is emitted alongside the
     *     remaining records from the same compressed message.
     *   - [[Decompressor.CorruptInput]] — the compressed stream is malformed or truncated. The
     *     entire compressed message is abandoned and `toBadRow` is invoked with message `"corrupt
     *     <format>-compressed payload"`.
     *   - [[Decompressor.UnsupportedVersionsInHeader]] — the Snowplow compression-format version
     *     byte is not `1`. The entire compressed message is abandoned and `toBadRow` is invoked
     *     with message `"unsupported versions in <format>-compressed record header: <v1>, <v2>"`.
     */
    def decompressedStream(
      processingConfig: EventProcessingConfig[F],
      decompressionConfig: DecompressionConfig,
      processor: DecompressedEventProcessor[F],
      badRowProcessor: BadRowProcessor,
      toBadRow: DecompressionError => BadRow
    ): Stream[F, Nothing] =
      underlying.stream(processingConfig, PayloadProvider.pipe(badRowProcessor, decompressionConfig, toBadRow).andThen(processor))
  }

  /**
   * Contextual information handed to `toBadRow` whenever a compressed message cannot be
   * decompressed.
   *
   * @param message
   *   Human-readable description of what went wrong (e.g. `"corrupt zstd-compressed payload"`).
   * @param timestamp
   *   The current time, pre-computed so the caller does not need to perform a side-effect.
   * @param payload
   *   The original compressed buffer, base64-encoded and ready to drop into the `payload` field of
   *   a `BadRow.RawPayload`.
   */
  case class DecompressionError(
    message: String,
    timestamp: Instant,
    payload: String
  )
}
