/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import org.specs2.Specification

class RecordGroupingSpec extends Specification {
  def is = s2"""
  RecordGrouping.group should
    return no groups for no records $e1
    keep every record exactly once $e2
    respect the record limit $e3
    respect the size limit $e4
    put a record bigger than the size limit alone in its own group $e5
    call getRecordSize once per record $e6
  """

  def e1 =
    RecordGrouping.group(List.empty[String], 10, 100)(_.length) must beEmpty

  def e2 = {
    val input  = (1 to 50).map(_.toString).toList
    val result = RecordGrouping.group(input, 7, 20)(_.length)
    result.flatten must containTheSameElementsAs(input)
  }

  def e3 = {
    val input  = List.fill(10)("x")
    val result = RecordGrouping.group(input, 3, 1000)(_.length)
    result.map(_.size).sorted must beEqualTo(List(1, 3, 3, 3))
  }

  def e4 = {
    val input  = List("aaaa", "bbbb", "cc", "dddddd", "e")
    val result = RecordGrouping.group(input, 100, 8)(_.length)
    val sizes  = result.map(_.map(_.length).sum)
    (sizes.sorted must beEqualTo(List(1, 8, 8))) and (result.flatten must containTheSameElementsAs(input))
  }

  def e5 = {
    val input  = List("ab", "this-is-too-big", "cd")
    val result = RecordGrouping.group(input, 100, 5)(_.length)
    result must contain(List("this-is-too-big"))
  }

  def e6 = {
    var calls = 0
    val input  = List("a", "bb", "ccc", "dddd")
    val result = RecordGrouping.group(input, 2, 100) { s => calls += 1; s.length }
    (calls, result.flatten.sorted) must beEqualTo((4, input))
  }
}
