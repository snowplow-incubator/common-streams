/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import java.nio.{ByteBuffer, ByteOrder}

/**
 * Compresses a collection of records into a single compressed frame.
 *
 * A single Compressor reuses its long-lived resources across many batches: call `reset` to begin a
 * batch, `addRecord` for each record, and `result` to obtain the compressed bytes. It is NOT
 * thread-safe; the caller must serialise access (a single fiber, or a Semaphore).
 */
trait Compressor {

  /**
   * Begin a new batch: prepare a fresh output frame and write the Snowplow header.
   *
   * `targetSize` is chosen per batch: it caps the compressed frame size and is the point at which
   * `addRecord` starts rejecting records. It only affects this batch's heap-side output buffer, not
   * the long-lived (off-heap) resources reused across batches, so it is cheap to vary from one
   * batch to the next.
   *
   * Precondition: `targetSize` must be large enough to hold the compression framing overhead (the
   * Snowplow header plus the format's own header/footer — on the order of a few tens of bytes).
   * Below that a valid frame cannot be produced and `result` throws.
   */
  def reset(payloadVersion: Int, targetSize: Int): Unit

  /**
   * Add a record to the current batch.
   *
   * @return
   *   false if adding the record would push the compressed frame past the target size, in which
   *   case the record is NOT added and the frame is rewound to before it.
   *
   * Once addRecord returns false, do not call it again for the current batch: take `result` to
   * obtain the batch, then `reset` before adding further records. Adding a record after a
   * rejection, or after `result`, throws IllegalStateException rather than silently corrupting the
   * output.
   */
  def addRecord(
    record: Array[Byte],
    offset: Int,
    len: Int
  ): Boolean

  /**
   * Finish the current batch and return its compressed bytes.
   *
   * Must be called after `reset` and before the next `reset`; calling it before a batch has begun,
   * or a second time on the same batch, throws IllegalStateException.
   */
  def result: ByteBuffer

  def recordCount: Int
}

object Compressor {

  /**
   * Format-specific machinery that turns records into compressed bytes appended to a sink. One
   * Engine instance is reused across batches; `begin` starts a fresh frame.
   */
  private[compression] trait Engine {
    def begin(sink: RewindableOutputStream): Unit
    def write(
      bytes: Array[Byte],
      off: Int,
      len: Int
    ): Unit
    def flush(): Unit
    def finish(): Unit
    def footerOverhead: Int

    // Optional hooks; only gzip needs them (its footer embeds an input count and CRC).
    def mark(): Unit         = ()
    def rewindToMark(): Unit = ()
    def commit(): Unit       = ()
  }

  /**
   * The lifecycle state of a Compressor, guarding against out-of-order calls that would otherwise
   * silently corrupt the frame. Singletons compared by reference (`eq`/`ne`) on the hot path.
   *
   * Transitions (any call not listed here throws IllegalStateException):
   *
   *   - Uninitialised --reset--> Open
   *   - Open --addRecord accepted--> Open
   *   - Open --addRecord rejected--> Full
   *   - Open --result--> Closed
   *   - Full --result--> Closed
   *   - Closed --reset--> Open
   *
   * So `addRecord` is only valid in `Open`, and `result` is valid in `Open` or `Full`.
   */
  private[compression] sealed trait State
  private[compression] object State {
    case object Uninitialised extends State
    case object Open extends State
    case object Full extends State
    case object Closed extends State
  }

  private[compression] final class Impl(engine: Engine) extends Compressor {
    import State._

    private var _recordCount: Int            = 0
    private var _targetSize: Int             = 0
    private var rwos: RewindableOutputStream = _
    private var state: State                 = Uninitialised

    override def recordCount: Int = _recordCount

    override def reset(payloadVersion: Int, targetSize: Int): Unit = {
      _recordCount = 0
      _targetSize  = targetSize
      rwos         = new RewindableOutputStream(targetSize)
      engine.begin(rwos)
      // Snowplow header: compression-format version, then payload-format version
      engine.write(Array[Byte](1, payloadVersion.toByte), 0, 2)
      state = Open
    }

    override def addRecord(
      record: Array[Byte],
      offset: Int,
      len: Int
    ): Boolean = {
      if (state ne Open)
        throw new IllegalStateException(
          s"addRecord is not valid in state $state (expected Open); call reset to begin a new batch"
        )
      // Mark the engine and the output stream, so we can rewind if we accidentally exceed the target output size
      engine.mark()
      rwos.mark()

      // Write 4 bytes, storing a 32-bit integer telling the reader the size of the record
      engine.write(sizeAsBytes(len), 0, 4)

      // Now write the record itself to the compressed stream
      engine.write(record, offset, len)

      // Flush the compressed stream, so that we can check the new total size of the compressed bytes
      engine.flush()

      // `footerOverhead` is needed because finishing the frame adds extra bytes to the output, depending on the algorithm
      if (rwos.size() + engine.footerOverhead > _targetSize) {
        // We have accidentally exceeded the target size, so rewind the engine and stream to the mark.  This `addRecord` was not successful.
        rwos.rewindToMark()
        engine.rewindToMark()
        // The batch is full: no more records may be added, but `result` is still expected next.
        state = Full
        false
      } else {
        // We have not exceeded the target size, so this `addRecord` was successful.
        engine.commit()
        _recordCount += 1
        true
      }
    }

    override def result: ByteBuffer = {
      if ((state ne Open) && (state ne Full))
        throw new IllegalStateException(
          s"result is not valid in state $state; call reset to begin a batch before taking its result"
        )
      engine.finish()
      state = Closed
      rwos.toByteBuffer()
    }
  }

  /** Creates 4 bytes representing a 32-bit big-endian integer. */
  private def sizeAsBytes(size: Int): Array[Byte] = {
    val bb = ByteBuffer.allocate(4)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(size)
    bb.array
  }
}
