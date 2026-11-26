/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import org.specs2.Specification

class ListOfListViewSpec extends Specification {
  def is = s2"""
  The view of a ListOfList should
    not invoke a mapping function until it is traversed $e1
  """

  def e1 = {
    val input = ListOfList.of(List(List(1, 2), Nil, List(3)))
    var calls = 0
    val mapped = input.view.map { i => calls += 1; i * 10 }
    val before = calls
    val result = mapped.toList

    (before, result, calls) must beEqualTo((0, List(10, 20, 30), 3))
  }
}
