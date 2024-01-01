package com.snowplowanalytics.snowplow.loaders.transform

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.badrows.Payload
import com.snowplowanalytics.snowplow.badrows.Processor
import io.circe.parser.decode
import org.specs2.Specification

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

class BadRowsSerializerSpec extends Specification {

  private val processor = Processor("test-app", "0.1.0")
  private val maxSize   = 3000

  def is = s2"""
   Bad row serialized should
    return original bad row if max size is not exceeded $e1
    return SizeViolation bad row if max size is exceeded $e2
  """

  def e1 = {
    val inputBadRow = loaderError(Event.minimal(UUID.randomUUID(), Instant.now(), "0.1.0", "0.1.0"))
    val output      = serialize(inputBadRow)

    decode[SelfDescribingData[BadRow]](output).map(_.data) must beRight(inputBadRow)
  }

  def e2 = {
    val inputBadRow = loaderError(Event.minimal(UUID.randomUUID(), Instant.now(), "0.1.0", "0.1.0").copy(mkt_source = Some("A" * 1000)))
    val output      = serialize(inputBadRow)

    decode[SelfDescribingData[BadRow]](output).map(_.data) must beRight.like { case sizeViolation: BadRow.SizeViolation =>
      sizeViolation.failure.maximumAllowedSizeBytes must beEqualTo(maxSize) and
        (sizeViolation.payload.event.size must beEqualTo(300)) // Max value divided by 10
    }
  }

  private def serialize(badRow: BadRow): String = {
    val output = BadRowsSerializer.withMaxSize(badRow, processor, maxSize)
    new String(output, StandardCharsets.UTF_8)
  }

  private def loaderError(event: Event) =
    BadRow.LoaderRuntimeError(processor, failure = "Some runtime loader error message", payload = Payload.LoaderPayload(event))
}
