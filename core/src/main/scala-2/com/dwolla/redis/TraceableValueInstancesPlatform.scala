package com.dwolla.redis

import cats.effect.Trace as _
import io.circe.*
import io.circe.syntax.*
import natchez.*

private[redis] trait TraceableValueInstancesPlatform {
  implicit def traceableValueViaJson[A: Encoder](implicit @annotation.unused ev: A =:!= String): TraceableValue[A] =
    TraceableValue[String].contramap(_.asJson.noSpaces)

  private def unexpected: Nothing = sys.error("Unexpected invocation")

  trait =:!=[A, B] extends Serializable

  implicit def neq[A, B]: A =:!= B = new=:!=[A, B] {}
  implicit def neqAmbig1[A]: A =:!= A = unexpected
  implicit def neqAmbig2[A]: A =:!= A = unexpected
}
