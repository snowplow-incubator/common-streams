/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks

import cats.{Eval, Foldable}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.specs2.Specification

class ListOfListSpec extends Specification {
  def is = s2"""
  An empty ListofList should
    identify itself as empty $empty1
    produce an empty Iterable $empty2
    foldMap to an empty result $empty3

  A non-empty ListOfList should
    provide an iterator covering all items in order $nonEmpty1
    allow prepending a batch $nonEmpty2
    map elements using a transformation lambda $nonEmpty3
    copy to a fs2 Chunk comprising all items $nonEmpty4
    copy to an IndexedSeq with the same order as the Iterable $nonEmpty5
    fold left over items $nonEmpty6
    fold right over items $nonEmpty7
    foldMap using a Monoid $nonEmpty8
    foldM using an Effect $nonEmpty9
  """

  def empty1 = {
    val input = ListOfList.empty
    input.isEmpty must beTrue
  }

  def empty2 = {
    val input = ListOfList.empty
    input.asIterable must beEmpty
  }

  def empty3 = {
    val input: ListOfList[Int] = ListOfList.empty
    val result                 = Foldable[ListOfList].foldMap(input)(_.toString)

    result must beEqualTo("")
  }

  def nonEmpty1 = {
    val input    = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6), Nil, List(7, 8, 9, 10)))
    val expected = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val result   = input.asIterable.toList

    result must beEqualTo(expected)
  }

  def nonEmpty2 = {
    val input    = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val addition = List(10, 11, 12)
    val expected = List(10, 11, 12, 1, 2, 3, 4, 5, 6)
    val result   = input.prepend(addition).asIterable.toList

    result must beEqualTo(expected)
  }

  def nonEmpty3 = {
    val input    = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val expected = List(100, 200, 300, 400, 500, 600)
    val result   = input.mapUnordered(_ * 100).asIterable.toList

    result must containTheSameElementsAs(expected)
  }

  def nonEmpty4 = {
    val input    = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val expected = List(1, 2, 3, 4, 5, 6)
    val result   = input.copyToChunk.toList

    result must containTheSameElementsAs(expected)
  }

  def nonEmpty5 = {
    val input   = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val result1 = input.copyToIndexedSeq.toVector
    val result2 = input.asIterable.toVector

    result1 must beEqualTo(result2)
  }

  def nonEmpty6 = {
    val input  = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val result = Foldable[ListOfList].foldLeft(input, "init")(_ + _.toString)

    result must beEqualTo("init123456")
  }

  def nonEmpty7 = {
    val input = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val result = Foldable[ListOfList].foldRight(input, Eval.now("init")) { case (next, acc) =>
      acc.map(_ + next.toString)
    }

    result.value must beEqualTo("init654321")
  }

  def nonEmpty8 = {
    val input  = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val result = Foldable[ListOfList].foldMap(input)(_.toString)

    result must beEqualTo("123456")
  }

  def nonEmpty9 = {
    val input = ListOfList.of(List(List(1, 2, 3), List(4, 5, 6)))
    val io = Foldable[ListOfList].foldM(input, "init") { case (acc, i) =>
      IO(s"$acc$i")

    }

    io.unsafeRunSync() must beEqualTo("init123456")
  }
}
