/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import cats.{Eval, Foldable, Monad, Monoid}

/**
 * An accumulator of sub-batches, for building a large batch cheaply and then folding over it.
 *
 * It is a `List[List[A]]`, where each inner list is one sub-batch. It is designed for
 * `BatchUp.Batchable.combine`, where small batches arrive one at a time: `prepend` adds a sub-batch
 * in O(1), without copying the elements already accumulated. Each method states its own cost.
 */
class ListOfList[+A](protected val lists: List[List[A]]) extends AnyVal with ListOfListVersionSpecific[A] {

  /** O(k) in the number of sub-batches */
  def isEmpty: Boolean = lists.forall(_.isEmpty)

  /** Prepends a sub-batch in O(1), without copying anything */
  def prepend[B >: A](elems: List[B]): ListOfList[B] =
    ListOfList.of(elems :: lists)

  /**
   * The number of elements.
   *
   * O(n + k) in the number of elements and sub-batches: it walks every sub-batch, without copying.
   * The size is not cached, so each call walks the batch again.
   */
  def size: Int =
    lists.foldLeft(0)(_ + _.size)

  /**
   * Applies `f` to every element, eagerly.
   *
   * A single pass over the elements, allocating one new cons cell per element and no intermediate
   * collections. The sub-batch structure is kept.
   */
  def map[B](f: A => B): ListOfList[B] =
    ListOfList.of(lists.map(_.map(f)))

  /**
   * Copies every element, in order, into a new `IndexedSeq`, in a single pass.
   *
   * Use it only when something needs fast lookup by index; to traverse the elements, use `view` or
   * fold instead.
   */
  def copyToIndexedSeq: IndexedSeq[A] = {
    val builder = Vector.newBuilder[A]
    lists.foreach(builder ++= _)
    builder.result()
  }
}

object ListOfList {

  def ofLists[A](elems: List[A]*): ListOfList[A] =
    new ListOfList(List(elems: _*))

  def of[A](value: List[List[A]]): ListOfList[A] =
    new ListOfList(value)

  val empty: ListOfList[Nothing] = new ListOfList(Nil)

  /** Inspired by the cats Foldable instance for List */
  implicit def listOfListFoldable: Foldable[ListOfList] = new Foldable[ListOfList] {

    override def toIterable[A](fa: ListOfList[A]): Iterable[A] =
      fa.view

    /**
     * cats' default `foldMap` combines one element at a time, which is O(n²) for monoids whose
     * `combine` copies the accumulator (`List`, `Vector`, `String`...). `combineAll` lets the
     * monoid build its result in a single linear pass.
     */
    override def foldMap[A, B](fa: ListOfList[A])(f: A => B)(implicit B: Monoid[B]): B =
      B.combineAll(fa.lists.iterator.flatMap(_.iterator).map(f))

    override def foldLeft[A, B](fa: ListOfList[A], b: B)(f: (B, A) => B): B =
      fa.lists.foldLeft(b) { case (acc, list) =>
        list.foldLeft(acc)(f)
      }

    override def foldRight[A, B](fa: ListOfList[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = {
      def loop(as: List[List[A]]): Eval[B] =
        as match {
          case Nil              => lb
          case Nil :: rest      => loop(rest)
          case (h :: t) :: rest => f(h, Eval.defer(loop(t :: rest)))
        }
      Eval.defer(loop(fa.lists))
    }

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

      fa.lists match {
        case Nil    => G.pure(z)
        case h :: t => G.tailRecM((h, t, z))(step)
      }
    }

  }
}
