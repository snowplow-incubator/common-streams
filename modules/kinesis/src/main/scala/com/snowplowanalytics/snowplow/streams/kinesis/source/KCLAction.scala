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

  final case class ProcessRecords(shardId: String, processRecordsInput: ProcessRecordsInput) extends KCLAction
  final case class ShardEnd(
    shardId: String,
    await: CountDownLatch,
    shardEndedInput: ShardEndedInput
  ) extends KCLAction
  final case class KCLError(t: Throwable) extends KCLAction

}
