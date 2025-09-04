package com.dwolla.redis

import cats.tagless.Derive
import cats.tagless.aop.{Aspect, Instrument}
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.effects
import natchez.TraceableValue

import com.dwolla.compat.scala.util.NotGiven
import org.typelevel.scalaccompat.annotation.unused
import scala.concurrent.duration.FiniteDuration

trait RedisCachePlatform extends RedisCachePlatformLowPriority {
  implicit def derivedAspect[K, V, Dom[_], Cod[_]](implicit @unused DK: Dom[K],
                                                   @unused DV: Dom[V],
                                                   @unused DFD: Dom[FiniteDuration],
                                                   @unused DEA: Dom[effects.SetArgs],
                                                   @unused CV: Cod[V],
                                                   @unused CB: Cod[Boolean],
                                                   @unused CL: Cod[Long],
                                                   @unused CU: Cod[Unit],
                                                   @unused COV: Cod[Option[V]],
                                                   @unused domNotTraceableValue: NotGiven[Dom[K] =:= TraceableValue[K]],
                                                   @unused codNotTraceableValue: NotGiven[Cod[V] =:= TraceableValue[K]],
                                                  ): Aspect[RedisCache[*[_], K, V], Dom, Cod] = Derive.aspect

  implicit def traceableValueAspect[K: TraceableValue, V: TraceableValue]: Aspect[RedisCache[*[_], K, V], TraceableValue, TraceableValue] =
    Derive.aspect
}

private sealed trait RedisCachePlatformLowPriority {
  implicit def instrument[K, V]: Instrument[RedisCache[*[_], K, V]] = Derive.instrument
}
