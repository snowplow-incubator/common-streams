/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import io.circe.Decoder
import io.circe.generic.semiauto._

/**
 * Configures behaviour of the parser when decompressing
 *
 * @param maxBytesInBatch
 *   A cutoff used when incrementally adding events to a batch. The batch is emitted immediately
 *   when this cutoff size is reached. This config parameter is needed to protect the app's memory.
 *   Bear in mind a 1MB compressed message could become HUGE after decompression.
 * @param maxBytesSinglePayload
 *   Each individual message should not exceed this size, after decompression.
 */
case class DecompressionConfig(maxBytesInBatch: Int, maxBytesSinglePayload: Int)

object DecompressionConfig {
  implicit val decoder: Decoder[DecompressionConfig] = deriveDecoder[DecompressionConfig].emap { config =>
    if (config.maxBytesInBatch <= 0)
      Left("maxBytesInBatch must be positive")
    else if (config.maxBytesSinglePayload <= 0)
      Left("maxBytesSinglePayload must be positive")
    else
      Right(config)
  }
}
