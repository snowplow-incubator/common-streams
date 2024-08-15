/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.runtime

import cats.Show

/**
 * Represents all messages from an Exception associated with a setup error
 *
 * Messages are expected to be included in the webhook payload. This library assumes each message
 * has been cleaned and sanitized by the Snowplow application.
 */
case class SetupExceptionMessages(value: List[String])

object SetupExceptionMessages {

  implicit def showSetupExceptionMessages: Show[SetupExceptionMessages] = {
    def removeDuplicateMessages(in: List[String]): List[String] =
      in match {
        case h :: t :: rest =>
          if (h.contains(t)) removeDuplicateMessages(h :: rest)
          else if (t.contains(h)) removeDuplicateMessages(t :: rest)
          else h :: removeDuplicateMessages(t :: rest)
        case fewer => fewer
      }

    Show.show { exceptionMessages =>
      removeDuplicateMessages(exceptionMessages.value).mkString(": ")
    }
  }
}
