package com.dwolla.redis

import cats.effect.Trace as _
import dev.profunktor.redis4cats.effects.*
import io.circe.*
import io.circe.literal.*
import io.circe.syntax.*
import natchez.*

import com.dwolla.compat.scala.util.NotGiven
import scala.concurrent.duration.*

private[redis] object TraceableValueInstances {
  implicit def optionalTraceableValue[A: TraceableValue]: TraceableValue[Option[A]] = {
    case Some(a) => TraceableValue[A].toTraceValue(a)
    case None => TraceableValue[String].toTraceValue("None")
  }

  implicit val unitTraceableValue: TraceableValue[Unit] = TraceableValue[String].contramap(_ => "()")

  implicit val finiteDurationTraceableValue: TraceableValue[FiniteDuration] = TraceableValue[String].contramap(_.toString())
  private implicit val existenceEncoder: Encoder[SetArg.Existence] = Encoder[String].contramap {
    case SetArg.Existence.Nx => "Nx"
    case SetArg.Existence.Xx => "Xx"
  }
  private implicit val ttlEncoder: Encoder[SetArg.Ttl] = Encoder[String].contramap {
    case SetArg.Ttl.Keep => "Keep"
    case SetArg.Ttl.Ex(duration) => s"${duration.toSeconds} seconds"
    case SetArg.Ttl.Px(duration) => s"${duration.toMillis} millis"
  }

  implicit val setArgsTraceableValue: TraceableValue[SetArgs] = TraceableValue[String].contramap { args =>
    json"""{
      "existence": ${args.existence.map(_.asJson)},
      "ttl": ${args.ttl.map(_.asJson)}
    }""".noSpaces
  }

  implicit def traceableValueViaJson[A: Encoder](implicit @annotation.unused aNotString: NotGiven[A =:= String],
                                                 @annotation.unused aNotBoolean: NotGiven[A =:= Boolean],
                                                 @annotation.unused aNotInt: NotGiven[A =:= Int],
                                                 @annotation.unused aNotLong: NotGiven[A =:= Long],
                                                 @annotation.unused aNotDouble: NotGiven[A =:= Double],
                                                 @annotation.unused aNotFloat: NotGiven[A =:= Float],
                                                ): TraceableValue[A] =
    TraceableValue[String].contramap(_.asJson.noSpaces)
}
