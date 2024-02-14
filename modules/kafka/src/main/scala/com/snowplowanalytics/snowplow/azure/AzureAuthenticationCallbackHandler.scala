/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.azure

import java.net.URI
import java.{lang, util}

import com.nimbusds.jwt.JWTParser

import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.core.credential.TokenRequestContext

trait AzureAuthenticationCallbackHandler extends AuthenticateCallbackHandler {

  val credentials = new DefaultAzureCredentialBuilder().build()

  var sbUri: String = ""

  override def configure(
    configs: util.Map[String, _],
    saslMechanism: String,
    jaasConfigEntries: util.List[AppConfigurationEntry]
  ): Unit = {
    val bootstrapServer =
      configs
        .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        .toString
        .replaceAll("\\[|\\]", "")
        .split(",")
        .toList
        .headOption match {
        case Some(s) => s
        case None    => throw new Exception("Empty bootstrap servers list")
      }
    val uri = URI.create("https://" + bootstrapServer)
    // Workload identity works with '.default' scope
    this.sbUri = s"${uri.getScheme}://${uri.getHost}/.default"
  }

  override def handle(callbacks: Array[Callback]): Unit =
    callbacks.foreach {
      case callback: OAuthBearerTokenCallback =>
        val token = getOAuthBearerToken()
        callback.token(token)
      case callback => throw new UnsupportedCallbackException(callback)
    }

  def getOAuthBearerToken(): OAuthBearerToken = {
    val reqContext = new TokenRequestContext()
    reqContext.addScopes(sbUri)
    val accessToken = credentials.getTokenSync(reqContext).getToken
    val jwt         = JWTParser.parse(accessToken)
    val claims      = jwt.getJWTClaimsSet

    new OAuthBearerToken {
      override def value(): String = accessToken

      override def lifetimeMs(): Long = claims.getExpirationTime.getTime

      override def scope(): util.Set[String] = null

      override def principalName(): String = null

      override def startTimeMs(): lang.Long = null
    }
  }

  override def close(): Unit = ()
}
