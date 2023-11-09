/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.Foldable
import cats.effect.IO
import org.specs2.Specification
import cats.effect.unsafe.implicits.global

import com.snowplowanalytics.snowplow.runtime.syntax.foldable._

class SyntaxSpec extends Specification {

  def is = s2"""
  The foldable syntax should
    Traverse a list and perform an effectful transformation $e1
    Traverse a list and perform an effectful transformation to Eithers $e2
    Sum elements of a list with an effect $e3
  """

  def e1 = {
    val inputs   = List("a", "b", "c")
    val expected = List("c-transformed", "b-transformed", "a-transformed")

    val result = Foldable[List]
      .traverseUnordered(inputs) { str =>
        IO.delay {
          s"$str-transformed"
        }
      }
      .unsafeRunSync()

    result must beEqualTo(expected)

  }

  def e2 = {
    val inputs         = List(1, 2, 3, 4, 5)
    val expectedLefts  = List(5, 3, 1)
    val expectedRights = List(4, 2)

    val (lefts, rights) = Foldable[List]
      .traverseSeparateUnordered(inputs) { i =>
        IO.delay {
          if (i % 2 > 0) Left(i) else Right(i)
        }
      }
      .unsafeRunSync()

    (lefts must beEqualTo(expectedLefts)) and
      (rights must beEqualTo(expectedRights))

  }

  def e3 = {
    val inputs = List[Long](1, 2, 3, 4, 5)

    val result = Foldable[List]
      .sumBy(inputs) { i =>
        i * 10
      }

    result must beEqualTo(150)
  }
}
