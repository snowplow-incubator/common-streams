/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import software.amazon.kinesis.lifecycle.events.{
  InitializationInput,
  LeaseLostInput,
  ProcessRecordsInput,
  ShardEndedInput,
  ShutdownRequestedInput
}
import software.amazon.kinesis.processor.{ShardRecordProcessor => KCLShardProcessor}

import java.util.concurrent.{CountDownLatch, SynchronousQueue}
import java.util.concurrent.atomic.AtomicReference

private[kinesis] object ShardRecordProcessor {

  def apply(
    queue: SynchronousQueue[KCLAction],
    currentShardIds: AtomicReference[Set[String]]
  ): KCLShardProcessor = new KCLShardProcessor {
    private var shardId: String = _

    override def initialize(initializationInput: InitializationInput): Unit = {
      shardId = initializationInput.shardId
      val oldSet = currentShardIds.getAndUpdate(_ + shardId)
      if (oldSet.contains(shardId)) {
        // This is a rare edge-case scenario. Three things must all happen to hit this scenario:
        //   1. KCL fails to renew a lease due to some transient runtime error
        //   2. KCL re-aquires the lost lease for the same shard
        //   3. The original ShardRecordProcessor is not terminated until after KCL re-aquires the lease
        // This is a very unhealthy state, so we should kill the app.
        val action = KCLAction.KCLError(new RuntimeException(s"Refusing to initialize a duplicate record processor for shard $shardId"))
        queue.put(action)
      }
    }

    override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
      val countDownLatch = new CountDownLatch(1)
      queue.put(KCLAction.ShardEnd(shardId, countDownLatch, shardEndedInput))
      countDownLatch.await()
      currentShardIds.updateAndGet(_ - shardId)
      ()
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val action = KCLAction.ProcessRecords(shardId, processRecordsInput)
      queue.put(action)
    }

    override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
      currentShardIds.updateAndGet(_ - shardId)
      ()
    }

    override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = ()

  }
}
