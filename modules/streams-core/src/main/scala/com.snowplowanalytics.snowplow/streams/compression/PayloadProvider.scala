/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.implicits._
import cats.effect.{Resource, Sync, Unique}
import fs2.{Chunk, Pipe, Pull, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, Payload => BadRowPayload, Processor => BadRowProcessor}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.util.Base64

import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.streams.compression.Decompression._

private[compression] object PayloadProvider {

  /**
   * Result returned by drainDecompressor when it needs to transition back to handleCompressedData
   *
   * @param pending
   *   A list of decompressed payloads, which will be emitted to the downstream application when the
   *   batch grows large enough
   * @param pendingByteCount
   *   Counts how many total bytes are in the `pending` list. Helpful for knowing when to emit a
   *   batch to the downstream app
   * @param pendingBad
   *   A list of bad rows, which will be emitted to the downstream application when the batch grows
   *   large enough
   */
  private case class DrainDecompressorResult(
    pending: List[ByteBuffer],
    pendingByteCount: Int,
    pendingBad: List[BadRow],
    payloadVersion: Option[Int]
  )

  /**
   * Opens a new decompressor using the provided factory and applies the given function to the
   * result. Ensures proper cleanup of the decompressor resource using bracketCase.
   *
   * @param factory
   *   The decompressor factory to use for opening the decompressor
   * @param data
   *   The compressed data to open the decompressor with
   * @param f
   *   The function to apply to the FactoryResult
   * @return
   *   A Pull that yields the DrainDecompressorResult from applying f to the FactoryResult
   */
  private def withOpenDecompressor[F[_]: Sync](
    factory: Decompressor.Factory,
    data: ByteBuffer
  )(
    f: Decompressor.FactoryResult => Pull[F, DecompressedTokenedEvents, DrainDecompressorResult]
  ): Pull[F, DecompressedTokenedEvents, DrainDecompressorResult] =
    Pull.bracketCase(
      Pull.pure(factory.build(data)),
      f,
      (factoryResult: Decompressor.FactoryResult, _: Resource.ExitCase) =>
        factoryResult match {
          case Decompressor.FactorySuccess(decompressor, _) =>
            Pull.eval(Sync[F].delay(decompressor.close()))
          case Decompressor.UnsupportedVersionsInHeader(_, _) =>
            Pull.done
        }
    )

  /**
   * Handles incoming stream messages and emits batches of payloads
   *
   *   - Incoming messages may be single payload per message; or multiple compressed payloads per
   *     message
   *   - A single incoming TokenedEvents might yield >1 output batches, if the batch becomes very
   *     big after decompression.
   *   - If a single incoming TokenedEvents yields >1 batch, then the `ack` token is attached to
   *     only the final batch from this input.
   *
   * Three failure modes are distinguished:
   *
   *   - [[Decompressor.RecordTooBig]] — a single record would exceed
   *     `decompressionConfig.maxBytesSinglePayload` after decompression. The record is skipped (the
   *     decompressor stays functional) and a `BadRow.SizeViolation` is emitted alongside the
   *     remaining records from the same compressed message. Built internally; not routed through
   *     `toBadRow`.
   *   - [[Decompressor.CorruptInput]] — the compressed stream is malformed or truncated. The entire
   *     compressed message is abandoned and `toBadRow` is invoked with message `"corrupt
   *     <format>-compressed payload"`.
   *   - [[Decompressor.UnsupportedVersionsInHeader]] — the Snowplow compression-format version byte
   *     is not `1`. The entire compressed message is abandoned and `toBadRow` is invoked with
   *     message `"unsupported versions in <format>-compressed record header: <v1>, <v2>"`.
   *
   * @param badRowProcessor
   *   Identifier for the `BadRow.SizeViolation` produced internally.
   * @param decompressionConfig
   *   Byte-size limits for decompression.
   * @param toBadRow
   *   Called when a compressed message cannot be decompressed (`CorruptInput` or
   *   `UnsupportedVersionsInHeader`). Receives a `DecompressionError` with a pre-rendered message,
   *   the current timestamp, and the base64-encoded original compressed buffer. Applications return
   *   whichever `BadRow` subtype fits their schema.
   */
  def pipe[F[_]: Sync](
    badRowProcessor: BadRowProcessor,
    decompressionConfig: DecompressionConfig,
    toBadRow: DecompressionError => BadRow
  ): Pipe[F, TokenedEvents, DecompressedTokenedEvents] =
    _.flatMap { tokenedEvents =>
      val sniffResult            = sniffAndSeparate(tokenedEvents.events)
      val outputFromUncompressed = streamUncompressed(sniffResult.uncompressed)
      val outputFromZstd = streamCompressed(
        decompressionConfig,
        new Decompressor.Zstd(decompressionConfig.maxBytesSinglePayload),
        badRowProcessor,
        toBadRow,
        sniffResult.zstd
      )
      val outputFromGzip = streamCompressed(
        decompressionConfig,
        new Decompressor.Gzip(decompressionConfig.maxBytesSinglePayload),
        badRowProcessor,
        toBadRow,
        sniffResult.gzip
      )
      val output: Stream[F, DecompressedTokenedEvents] = outputFromUncompressed ++ outputFromZstd ++ outputFromGzip
      output.through(appendTokenToLast(tokenedEvents.ack))
    }

  /**
   * Appends the `ack` token to the last output batch corresponding to a single incoming
   * `TokenedEvents`
   */
  private def appendTokenToLast[F[_]](token: Unique.Token): Pipe[F, DecompressedTokenedEvents, DecompressedTokenedEvents] = {
    def pull(
      stream: Stream[F, DecompressedTokenedEvents],
      toEmit: Option[DecompressedTokenedEvents]
    ): Pull[F, DecompressedTokenedEvents, Unit] =
      stream.pull.uncons1.flatMap {
        case None =>
          toEmit match {
            case Some(result) =>
              Pull.output1(result.copy(ack = Some(token)))
            case None =>
              Pull.output1(DecompressedTokenedEvents(Nil, Nil, Some(token), None))
          }
        case Some((pulled, next)) =>
          Pull.outputOption1(toEmit) >> pull(next, Some(pulled))
      }

    in => pull(in, None).stream
  }

  private def streamUncompressed[F[_]](uncompressed: List[ByteBuffer]): Stream[F, DecompressedTokenedEvents] =
    if (uncompressed.nonEmpty)
      Stream.emit(DecompressedTokenedEvents(uncompressed, Nil, None, None))
    else
      Stream.empty

  /**
   * Decompresses received compressed stream messages and emits [[DecompressedTokenedEvents]] to the
   * downstream application
   *
   * @param config
   *   Configures details of the decompression
   * @param badProcessor
   *   Bad row processor used when we need to create a bad row
   * @param factory
   *   Used to open a new `Decompressor` for each received stream message
   * @param compressed
   *   A list of compressed messages received from the input stream. We have already checked that
   *   these messages are compressed with a supported compression algorithm.
   */
  private def streamCompressed[F[_]: Sync](
    config: DecompressionConfig,
    factory: Decompressor.Factory,
    badProcessor: BadRowProcessor,
    toBadRow: DecompressionError => BadRow,
    compressed: List[ByteBuffer]
  ): Stream[F, DecompressedTokenedEvents] = {

    /*
     * Processes a queue of compressed data by opening new decompressors as needed.
     *
     * This function handles the case when no active decompressor exists. It either:
     * - Emits final results if no more compressed data remains
     * - Opens a new decompressor for the next compressed message and delegates to drainDecompressor
     * - Creates bad rows for unsupported compression formats
     *
     * @param remainder The subset of compressed data which have not been processed yet
     * @param pending A list of decompressed payloads, which will be emitted to the downstream application when the batch grows large enough
     * @param pendingByteCount Counts how many total bytes are in the `pending` list. Helpful for knowing when to emit a batch to the downstream app
     * @param pendingBad A list of bad rows, which will be emitted to the downstream application when the batch grows large enough
     * @param payloadVersion The payload format version from the most recently opened decompressor.
     *   When a compressed message has unsupported versions, the payloadVersion from the previous
     *   successful decompressor is preserved. In practice all compressed messages in a single
     *   TokenedEvents should use the same payload format version.
     */
    def handleCompressedData(
      remainder: List[ByteBuffer],
      pending: List[ByteBuffer],
      pendingByteCount: Int,
      pendingBad: List[BadRow],
      payloadVersion: Option[Int]
    ): Pull[F, DecompressedTokenedEvents, Unit] =
      remainder match {
        case Nil =>
          if (pending.nonEmpty || pendingBad.nonEmpty) Pull.output1(DecompressedTokenedEvents(pending, pendingBad, None, payloadVersion))
          else Pull.done
        case head :: tail =>
          withOpenDecompressor(factory, head.slice()) {
            case Decompressor.FactorySuccess(decompressor, fv) =>
              drainDecompressor(pending, pendingByteCount, pendingBad, decompressor, head, fv)
            case Decompressor.UnsupportedVersionsInHeader(v1, v2) =>
              val message = show"unsupported versions in ${factory.format}-compressed record header: $v1, $v2"
              for {
                bad <- Pull.eval(buildFailureBadRow(toBadRow, message, head.slice()))
              } yield DrainDecompressorResult(pending, pendingByteCount, bad :: pendingBad, payloadVersion)
          }.flatMap { case DrainDecompressorResult(pending, pendingByteCount, pendingBad, fv) =>
            handleCompressedData(tail, pending, pendingByteCount, pendingBad, fv)
          }
      }

    /*
     * Extracts all available records from an active decompressor.
     *
     * This function handles the case when we have an active decompressor. It either:
     * - Extracts the next record and continues draining (possibly emitting batches when they get large)
     * - Handles decompressor end-of-records by closing it and returning state for handleCompressedData
     * - Creates bad rows for records that are too big or corrupt input
     *
     * @param pending A list of decompressed payloads, which will be emitted to the downstream application when the batch grows large enough
     * @param pendingByteCount Counts how many total bytes are in the `pending` list. Helpful for knowing when to emit a batch to the downstream app
     * @param pendingBad A list of bad rows, which will be emitted to the downstream application when the batch grows large enough
     * @param decompressor The active `Decompressor` which provides decompressed payloads from the in-progress stream message
     * @param original The original `ByteBuffer` which was used to open the in-progress `Decompressor`. Only used for creating bad rows if something goes wrong.
     * @param payloadVersion The payload format version from the compression header
     */
    def drainDecompressor(
      pending: List[ByteBuffer],
      pendingByteCount: Int,
      pendingBad: List[BadRow],
      decompressor: Decompressor,
      original: ByteBuffer,
      payloadVersion: Int
    ): Pull[F, DecompressedTokenedEvents, DrainDecompressorResult] =
      decompressor.getNextRecord match {
        case Decompressor.Record(bytes) =>
          val bb = ByteBuffer.wrap(bytes)
          if (pendingByteCount + bytes.size > config.maxBytesInBatch)
            // maxBytesInBatch config param exists to protect us from running out of memory. But, on this line we have
            // already decompressed the next record, therefore we are already "using" the memory. So it doesn't really
            // matter if we add it to the pending batch. Therefore, we are treating maxBytesInBatch more like a guideline
            // rather than a strict limit.
            Pull.output1(DecompressedTokenedEvents(bb :: pending, pendingBad, None, Some(payloadVersion))) >>
              drainDecompressor(Nil, 0, Nil, decompressor, original, payloadVersion)
          else
            drainDecompressor(bb :: pending, pendingByteCount + bytes.size, pendingBad, decompressor, original, payloadVersion)

        case Decompressor.EndOfRecords =>
          Pull.pure(DrainDecompressorResult(pending, pendingByteCount, pendingBad, Some(payloadVersion)))

        case Decompressor.RecordTooBig(size) =>
          for {
            bad <- Pull.eval(sizeViolationBadRow(config, badProcessor, factory.format, original, size))
            result <- drainDecompressor(pending, pendingByteCount, bad :: pendingBad, decompressor, original, payloadVersion)
          } yield result

        case Decompressor.CorruptInput =>
          val message = show"corrupt ${factory.format}-compressed payload"
          for {
            bad <- Pull.eval(buildFailureBadRow(toBadRow, message, original.slice()))
          } yield DrainDecompressorResult(pending, pendingByteCount, bad :: pendingBad, Some(payloadVersion))
      }

    handleCompressedData(compressed, Nil, 0, Nil, None).stream
  }

  /**
   * The result of sniffing the first few bytes of an incoming `TokenedEvents`
   *
   * @param uncompressed
   *   The subset of the incoming stream messages which we believe are uncompressed.
   * @param zstd
   *   The subset of the incoming stream messages which we believe are zstd-compressed. These bytes
   *   still needs to be decompressed and parsed to yield payloads.
   * @param gzip
   *   The subset of the incoming stream messages which we believe are gzipped-compressed. These
   *   bytes still needs to be decompressed and parsed to yield payloads.
   */
  private final case class SniffAndSeparateResult(
    uncompressed: List[ByteBuffer],
    zstd: List[ByteBuffer],
    gzip: List[ByteBuffer]
  )

  /**
   * If a byte array starts with hex string `28 b5 2f fd` then we infer these are zstd-compressed
   * bytes
   */
  private val zstdSignature: Array[Byte] = Array[Int](0x28, 0xb5, 0x2f, 0xfd).map(_.toByte)

  /**
   * If a byte array starts with hex string `1f 8b` then we infer these are gzip-compressed bytes
   */
  private val gzipSignature: Array[Byte] = Array[Int](0x1f, 0x8b).map(_.toByte)

  /**
   * Sniffs the first few bytes of the incoming stream message, and separates according to
   * how/whether it is compressed.
   */
  private def sniffAndSeparate(inputs: Chunk[ByteBuffer]): SniffAndSeparateResult =
    inputs.foldLeft(SniffAndSeparateResult(Nil, Nil, Nil)) { case (acc, buffer) =>
      if (matchesPrefix(buffer, zstdSignature))
        acc.copy(zstd = buffer :: acc.zstd)
      else if (matchesPrefix(buffer, gzipSignature))
        acc.copy(gzip = buffer :: acc.gzip)
      else
        acc.copy(uncompressed = buffer :: acc.uncompressed)
    }

  private def matchesPrefix(input: ByteBuffer, knownPrefix: Array[Byte]): Boolean =
    if (input.remaining < knownPrefix.length)
      false
    else {
      val slice = input.slice
      (0 until knownPrefix.size).forall { i =>
        knownPrefix(i) === slice.get(i)
      }
    }

  private def toBase64String(
    bytes: ByteBuffer
  ): String =
    StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(bytes)).toString

  private def buildFailureBadRow[F[_]: Sync](
    toBadRow: DecompressionError => BadRow,
    message: String,
    original: ByteBuffer
  ): F[BadRow] =
    Sync[F].realTimeInstant.map { now =>
      toBadRow(DecompressionError(message, now, toBase64String(original)))
    }

  private def sizeViolationBadRow[F[_]: Sync](
    config: DecompressionConfig,
    badProcessor: BadRowProcessor,
    format: String,
    original: ByteBuffer,
    size: Int
  ): F[BadRow.SizeViolation] =
    Sync[F].realTimeInstant.map { now =>
      val msg =
        show"Payload will exceed maximum allowed size of ${config.maxBytesSinglePayload} after $format decompression"
      BadRow.SizeViolation(
        badProcessor,
        BadRowFailure.SizeViolation(now, config.maxBytesSinglePayload, size, msg),
        BadRowPayload.RawPayload(toBase64String(original))
      )
    }

}
