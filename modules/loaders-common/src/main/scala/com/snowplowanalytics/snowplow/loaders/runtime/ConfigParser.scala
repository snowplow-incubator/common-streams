/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.runtime

import cats.data.EitherT
import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.typesafe.config.{Config => TypesafeConfig, ConfigFactory}
import io.circe.{Decoder, Json}
import io.circe.config.syntax.CirceConfigOps

import scala.jdk.CollectionConverters._

import java.nio.file.{Files, Path}

object ConfigParser {

  /**
   * Parse Iglu resolver config from a file, or return an appropriate error message
   *
   * @param path
   *   The location of the file
   * @return
   *   The parsed config, or an appropriate error message to display to the end user
   */
  def igluResolverFromFile[F[_]: Sync](path: Path): EitherT[F, String, Resolver.ResolverConfig] =
    for {
      resolverJson <- configFromFile[F, Json](path)
      resolver <- resolverFromJson(resolverJson)
    } yield resolver

  /**
   * Parse the application config from a file, or return an appropriate error message
   *
   * @tparam A
   *   The application-specific config class
   * @param path
   *   The location of the file
   * @return
   *   The parsed config, or an appropriate error message to display to the end user
   */
  def configFromFile[F[_]: Sync, A: Decoder](path: Path): EitherT[F, String, A] =
    for {
      text <- EitherT(readTextFrom[F](path))
      hocon <- EitherT.fromEither[F](hoconFromString(text))
      result <- EitherT.fromEither[F](resolve(hocon))
    } yield result

  private def resolverFromJson[F[_]: Sync](json: Json): EitherT[F, String, Resolver.ResolverConfig] =
    EitherT
      .fromEither[F](Resolver.parseConfig(json))
      .leftMap(_.show)

  private def readTextFrom[F[_]: Sync](path: Path): F[Either[String, String]] =
    Sync[F].blocking {
      Either
        .catchNonFatal(Files.readAllLines(path).asScala.mkString("\n"))
        .leftMap(e => s"Error reading ${path.toAbsolutePath} file from filesystem: ${e.getMessage}")
    }

  private def hoconFromString(str: String): Either[String, TypesafeConfig] =
    Either
      .catchNonFatal(ConfigFactory.parseString(str))
      .leftMap(_.getMessage)

  private def resolve[A: Decoder](hocon: TypesafeConfig): Either[String, A] = {
    val either = for {
      resolved <- Either.catchNonFatal(hocon.resolve()).leftMap(_.getMessage)
      resolved <- Either.catchNonFatal(loadAll(resolved)).leftMap(_.getMessage)
      parsed <- resolved.as[A].leftMap(_.show)
    } yield parsed
    either.leftMap(e => s"Cannot resolve config: $e")
  }

  private def loadAll(config: TypesafeConfig): TypesafeConfig =
    namespaced(ConfigFactory.load(namespaced(config.withFallback(namespaced(ConfigFactory.load())))))

  private def namespaced(config: TypesafeConfig): TypesafeConfig = {
    val namespace = "snowplow"
    if (config.hasPath(namespace))
      config.getConfig(namespace).withFallback(config.withoutPath(namespace))
    else
      config
  }

}
