/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

/** Splits a sink's records into requests that respect the external service's limits */
private[streams] object RecordGrouping {

  /**
   * Splits `records` into groups, each as big as possible while respecting `recordLimit` and
   * `sizeLimit`.
   *
   * It traverses `records` once and calls `getRecordSize` once per record. Order is not preserved,
   * neither of the groups nor of the records within a group. A record bigger than `sizeLimit` goes
   * alone into its own group; the caller's external service decides what to do with it.
   */
  def group[A](
    records: Iterable[A],
    recordLimit: Int,
    sizeLimit: Int
  )(
    getRecordSize: A => Int
  ): List[List[A]] =
    records
      .foldLeft(List.empty[GroupAcc[A]]) { case (acc, record) =>
        val recordSize = getRecordSize(record)
        acc match {
          case head :: tail if head.count + 1 <= recordLimit && head.size + recordSize <= sizeLimit =>
            GroupAcc(head.size + recordSize, head.count + 1, record :: head.records) :: tail
          case _ =>
            GroupAcc(recordSize, 1, List(record)) :: acc
        }
      }
      .map(_.records)

  private case class GroupAcc[A](
    size: Int,
    count: Int,
    records: List[A]
  )
}
