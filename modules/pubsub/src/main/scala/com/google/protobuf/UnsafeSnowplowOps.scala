/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.google.protobuf

object UnsafeSnowplowOps {

  /* Wrap a (technical mutable) array of bytes as an immutable ByteString
   *
   * The protobuf library hides this method from us, because a `ByteString` MUST be immutable. In
   * Snowplow apps, by convention we never mutate an array of bytes after serializing a message for
   * pubsub. So we can justify calling this unsafe method.
   */
  def wrapBytes(bytes: Array[Byte]): ByteString = ByteString.wrap(bytes)
}
