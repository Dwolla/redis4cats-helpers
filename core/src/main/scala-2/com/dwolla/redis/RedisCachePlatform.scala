package com.dwolla.redis

import cats.tagless.Derive
import cats.tagless.aop.{Aspect, Instrument}
import natchez.TraceableValue
import com.dwolla.redis.TraceableValueInstances.*

trait RedisCachePlatform {
  implicit def instrument[K, V]: Instrument[RedisCache[*[_], K, V]] = Derive.instrument
  implicit def aspect[K: TraceableValue, V: TraceableValue]: Aspect[RedisCache[*[_], K, V], TraceableValue, TraceableValue] = Derive.aspect
}
