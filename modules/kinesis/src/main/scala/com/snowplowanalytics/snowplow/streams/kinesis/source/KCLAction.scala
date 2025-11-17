/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis.source

import software.amazon.kinesis.lifecycle.events.{ProcessRecordsInput, ShardEndedInput}

import java.util.concurrent.CountDownLatch

private sealed trait KCLAction

private object KCLAction {

  /**
   * The action emitted by the ShardRecordProcessor when it receives new records
   *
   * @param await
   *   A countdown latch used to backpressure the ShardRecordProcessor. The consumer of the queue
   *   should release the countdown latch to unblock the ShardRecordProcessor and let it fetch more
   *   records from Kinesis.
   */
  final case class ProcessRecords(
    shardId: String,
    await: CountDownLatch,
    processRecordsInput: ProcessRecordsInput
  ) extends KCLAction

  /**
   * The action emitted by the ShardRecordProcessor when it reaches a shard end.
   *
   * @param await
   *   A countdown latch used to block the ShardRecordProcessor until all records from this shard
   *   have been checkpointed.
   *
   * @note
   *   Unlike the `await` in the `ProcessRecords` class, this countdown latch must not be released
   *   immediately by the queue consumer. It must only be released by the checkpointer.
   */
  final case class ShardEnd(
    shardId: String,
    await: CountDownLatch,
    shardEndedInput: ShardEndedInput
  ) extends KCLAction

  final case class KCLError(
    t: Throwable,
    await: CountDownLatch
  ) extends KCLAction

}
