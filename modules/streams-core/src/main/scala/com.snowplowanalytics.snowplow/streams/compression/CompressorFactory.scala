/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.effect.{Resource, Sync}

import com.github.luben.zstd.ZstdCompressCtx

import java.util.zip.Deflater

/**
 * Builds a reusable [[Compressor]] as a Resource. The Resource owns the long-lived (off-heap)
 * resources: the native deflater for gzip, or the native compression context for zstd, freed once
 * on release.
 *
 * @see
 *   Compressor for the (serial, not-thread-safe) usage contract.
 */
sealed trait CompressorFactory {
  def resource[F[_]: Sync]: Resource[F, Compressor]
}

object CompressorFactory {

  def gzip(compressionLevel: Int): CompressorFactory =
    new CompressorFactory {
      override def resource[F[_]: Sync]: Resource[F, Compressor] =
        Resource
          // nowrap = true: the engine writes the gzip header and trailer itself, so the deflater emits raw deflate
          .make(Sync[F].delay(new Deflater(compressionLevel, true)))(d => Sync[F].delay(d.end()))
          .map(d => new Compressor.Impl(new GzipEngine(d)))
    }

  def zstd(compressionLevel: Int): CompressorFactory =
    new CompressorFactory {
      override def resource[F[_]: Sync]: Resource[F, Compressor] =
        Resource
          .make(Sync[F].delay(new ZstdCompressCtx))(ctx => Sync[F].delay(ctx.close()))
          .map(ctx => new Compressor.Impl(new ZstdEngine(ctx, compressionLevel)))
    }
}
