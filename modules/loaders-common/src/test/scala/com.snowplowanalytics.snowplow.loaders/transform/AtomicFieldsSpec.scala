/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import org.specs2.Specification

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import java.util.UUID
import java.time.Instant

class AtomicFieldsSpec extends Specification {
  def is = s2"""
  AtomicFields should contain a Field for each atomic field of a Snowplow Event $e1
  """

  def e1 = {
    val sdkEvent = Event.minimal(UUID.randomUUID, Instant.now, "0.0.0", "0.0.0")

    val result = AtomicFields.static.map(_.name)

    val expected = sdkEvent.atomic.keys.toSeq

    result must containTheSameElementsAs(expected)
  }

}
