package com.snowplowanalytics.snowplow.loaders.transform

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import java.nio.charset.StandardCharsets
import java.time.Instant

object BadRowsSerializer {

  /**
   * If input bad row exceeds provided max size in bytes, return serialized SizeViolation bad row
   * with trimmed original payload. If not, return original serialized bad row.
   */
  def withMaxSize(
    badRow: BadRow,
    processor: Processor,
    maxSize: Int
  ): Array[Byte] = {
    val originalSerialized = badRow.compactByteArray
    val originalSizeBytes  = originalSerialized.length

    if (originalSizeBytes >= maxSize) {
      val trimmedPayload = new String(originalSerialized, StandardCharsets.UTF_8).take(maxSize / 10)
      BadRow
        .SizeViolation(
          processor,
          Failure.SizeViolation(Instant.now(), maxSize, originalSizeBytes, "Badrow exceeds allowed max size"),
          Payload.RawPayload(trimmedPayload)
        )
        .compactByteArray
    } else {
      originalSerialized
    }
  }
}
