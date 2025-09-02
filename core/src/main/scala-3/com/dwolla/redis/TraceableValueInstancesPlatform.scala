package com.dwolla.redis

import io.circe.Encoder
import io.circe.syntax.*
import natchez.TraceableValue
import scala.util.NotGiven

private[redis] trait TraceableValueInstancesPlatform:
  implicit def traceableValueViaJson[A](using Encoder[A], NotGiven[A =:= String]): TraceableValue[A] =
    TraceableValue[String].contramap(_.asJson.noSpaces)
