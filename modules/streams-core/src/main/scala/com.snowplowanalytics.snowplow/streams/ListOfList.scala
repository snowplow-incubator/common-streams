/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import fs2.Chunk
import cats.implicits._
import cats.{Eval, Foldable, Monad, Monoid}
import scala.collection.compat._

/**
 * A data structure that is efficient for most Snowplow streaming apps
 *
 * This is implemented as a `List[List[A]]`. But the inner Lists are hidden from the developer, so
 * to force us into only using efficient methods.
 *
 * A `ListOfList` has these features:
 *
 *   - **Fast prepend** when building bigger batches from smaller batches e.g.
 *     `batchesOfEvents.prepend(anotherBatch)`.
 *   - **Fast folding** e.g. Foldable[ListOfList].foldMap(batches)(event => ???)
 *
 * It is ideal for situations where:
 *
 *   - We don't care about order of events within a batch
 *   - We want to minimize how often we copy data structures
 *   - We don't need fast lookup by index
 *   - We want to batch up small batches into large batches of events
 *
 * It is deliberately missing a few features, so to force us into efficient usage patterns:
 *
 *   - No `.size` or `.length` methods. In Snowplow apps we manage batch size by other means.
 *   - No `.traverse`. Instead we can use:
 *
 * ```
 * Foldable[ListOfList].traverseUnordered(listOfList)(a => IO { ??? })
 * ```
 */
class ListOfList[+A](private val value: List[List[A]]) extends AnyVal {

  def isEmpty: Boolean = value.forall(_.isEmpty)

  /** Fast prepend a batch to the beginning of this ListOfList */
  def prepend[B >: A](elems: List[B]): ListOfList[B] =
    ListOfList.of(elems :: value)

  /**
   * Apply a transformation function `f` to every element in the ListOfList
   *
   * The resulting `ListOfList` does not have the same order as the input List. This is helpful in
   * Snowplow apps where order of events within batches is not important.
   */
  def mapUnordered[B](f: A => B): ListOfList[B] =
    ListOfList.of {
      List {
        value.foldLeft(List.empty[B]) { case (bs, list) =>
          list.foldLeft(bs) { case (bs, a) =>
            f(a) :: bs
          }
        }
      }
    }

  /**
   * An `Iterable` which is a lightweight wrapper over the underlying `ListOfList`.
   *
   * This is efficient because it does not do a copy of the data structure
   */
  def asIterable: Iterable[A] =
    Iterable.from(value.foldLeft[Iterator[A]](Iterator.empty)(_ ++ _))

  /**
   * Converts the ListOfList to a `fs2.Chunk`.
   *
   * This does an inefficient copy of the underlying data, and so should only be used when a 3rd
   * party library requires a `Chunk`.
   */
  def copyToChunk: Chunk[A] =
    Chunk.from(value).flatMap(Chunk.from(_))

  /**
   * Converts the ListOfList to an IndexedSeq
   *
   * This does an inefficient copy of the underlying data, and so should only be used when we need
   * to fast lookup by index, for a range of indexes.
   */
  def copyToIndexedSeq: IndexedSeq[A] =
    asIterable.toIndexedSeq

  /**
   * This function takes a list of records and splits it into several lists, where each list is as
   * big as possible with respecting the record limit and the size limit.
   */
  def group(
    recordLimit: Int,
    sizeLimit: Int,
    getRecordSize: A => Int
  ): List[List[A]] = {
    import ListOfList.GroupAcc

    this
      .foldLeft(List.empty[GroupAcc[A]]) { case (acc, record) =>
        val recordSize = getRecordSize(record)
        acc match {
          case head :: tail =>
            if (head.count + 1 > recordLimit || head.size + recordSize > sizeLimit)
              List(GroupAcc(recordSize, 1, List(record))) ++ List(head) ++ tail
            else
              List(GroupAcc(head.size + recordSize, head.count + 1, record :: head.records)) ++ tail
          case Nil =>
            List(GroupAcc(recordSize, 1, List(record)))
        }
      }
      .map(_.records)
  }
}

object ListOfList {

  def ofItems[A](elems: A*): ListOfList[A] =
    new ListOfList(List(List(elems: _*)))

  def ofLists[A](elems: List[A]*): ListOfList[A] =
    new ListOfList(List(elems: _*))

  def of[A](value: List[List[A]]): ListOfList[A] =
    new ListOfList(value)

  val empty: ListOfList[Nothing] = new ListOfList(Nil)

  /** Inspired by the cats Foldable instance for List */
  implicit def listOfListFoldable: Foldable[ListOfList] = new Foldable[ListOfList] {

    override def toIterable[A](fa: ListOfList[A]): Iterable[A] =
      fa.asIterable

    override def foldLeft[A, B](fa: ListOfList[A], b: B)(f: (B, A) => B): B =
      fa.value.foldLeft(b) { case (acc, list) =>
        list.foldLeft(acc)(f)
      }

    override def foldRight[A, B](fa: ListOfList[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = {
      def loop(as: List[List[A]]): Eval[B] =
        as match {
          case Nil              => lb
          case Nil :: rest      => loop(rest)
          case (h :: t) :: rest => f(h, Eval.defer(loop(t :: rest)))
        }
      Eval.defer(loop(fa.value))
    }

    override def foldMap[A, B](fa: ListOfList[A])(f: A => B)(implicit B: Monoid[B]): B =
      B.combineAll(toIterable(fa).map(f))

    override def foldM[G[_], A, B](fa: ListOfList[A], z: B)(f: (B, A) => G[B])(implicit G: Monad[G]): G[B] = {
      def step(in: (List[A], List[List[A]], B)): G[Either[(List[A], List[List[A]], B), B]] =
        in match {
          case (Nil, Nil, b)    => G.pure(Right(b))
          case (Nil, h :: t, b) => step((h, t, b))
          case (h :: t, rest, b) =>
            G.map(f(b, h)) { bnext =>
              Left((t, rest, bnext))
            }
        }

      fa.value match {
        case Nil    => G.pure(z)
        case h :: t => G.tailRecM((h, t, z))(step)
      }
    }

  }

  private case class GroupAcc[A](
    size: Int,
    count: Int,
    records: List[A]
  )
}
