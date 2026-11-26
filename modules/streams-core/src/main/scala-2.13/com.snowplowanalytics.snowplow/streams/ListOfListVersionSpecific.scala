/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import scala.collection.View

/** The parts of [[ListOfList]] whose types differ between Scala 2.12 and 2.13 */
trait ListOfListVersionSpecific[+A] extends Any {

  protected def lists: List[List[A]]

  /**
   * A lazy, re-iterable view over every element, in order.
   *
   * This does not copy anything. Transformations on the view, e.g. `view.map(f)`, are lazy and run
   * again on every traversal.
   */
  def view: View[A] =
    View.fromIteratorProvider(() => lists.iterator.flatMap(_.iterator))
}
