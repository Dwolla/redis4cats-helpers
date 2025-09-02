package com.dwolla.redis

import cats.tagless.aop.{Aspect, Instrument, Instrumentation}
import cats.~>
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.effects
import natchez.TraceableValue

import scala.concurrent.duration.FiniteDuration

trait RedisCachePlatform {
  implicit def instrument[K, V]: Instrument[[F[_]] =>> RedisCache[F, K, V]] =
    new Instrument[[F[_]] =>> RedisCache[F, K, V]]:
      override def instrument[F[_]](af: RedisCache[F, K, V]): RedisCache[Instrumentation[F, *], K, V] =
        new RedisCache[Instrumentation[F, *], K, V]:
          override def get(key: K): Instrumentation[F, Option[V]] =
            Instrumentation(af.get(key), "RedisCache", "get")

          override def set(key: K, value: V, setArgs: effects.SetArgs): Instrumentation[F, Boolean] =
            Instrumentation(af.set(key, value, setArgs), "RedisCache", "set")

          override def del(key: K): Instrumentation[F, Long] =
            Instrumentation(af.del(key), "RedisCache", "del")

          override def updateExisting(key: K, existing: V)
                                     (newValue: V, expiresIn: FiniteDuration): Instrumentation[F, Unit] =
            Instrumentation(af.updateExisting(key, existing)(newValue, expiresIn), "RedisCache", "updateExisting")

          override def deleteExisting(key: K, existing: V): Instrumentation[F, Unit] =
            Instrumentation(af.deleteExisting(key, existing), "RedisCache", "deleteExisting")

      override def mapK[F[_], G[_]](af: RedisCache[F, K, V])
                                   (fk: F ~> G): RedisCache[G, K, V] =
        new RedisCacheMapKd(af, fk)

  implicit def aspect[K: TraceableValue, V: TraceableValue]: Aspect[[F[_]] =>> RedisCache[F, K, V], TraceableValue, TraceableValue] =
    new Aspect[[F[_]] =>> RedisCache[F, K, V], TraceableValue, TraceableValue]:
      override def weave[F[_]](af: RedisCache[F, K, V]): RedisCache[Aspect.Weave[F, TraceableValue, TraceableValue, *], K, V] =
        new RedisCache[Aspect.Weave[F, TraceableValue, TraceableValue, *], K, V]:
          override def get(key: K): Aspect.Weave[F, TraceableValue, TraceableValue, Option[V]] =
            Aspect.Weave("RedisCache", List(List(Aspect.Advice.byValue("key", key))), Aspect.Advice("get", af.get(key)))

          override def set(key: K, value: V, setArgs: effects.SetArgs): Aspect.Weave[F, TraceableValue, TraceableValue, Boolean] =
            Aspect.Weave("RedisCache", List(List(
              Aspect.Advice.byValue("key", key),
              Aspect.Advice.byValue("value", value),
              Aspect.Advice.byValue("setArgs", setArgs),
            )), Aspect.Advice("set", af.set(key, value, setArgs)))

          override def del(key: K): Aspect.Weave[F, TraceableValue, TraceableValue, Long] =
            Aspect.Weave("RedisCache", List(List(Aspect.Advice.byValue("key", key))), Aspect.Advice("del", af.del(key)))

          override def updateExisting(key: K, existing: V)
                                     (newValue: V, expiresIn: FiniteDuration): Aspect.Weave[F, TraceableValue, TraceableValue, Unit] =
            Aspect.Weave("RedisCache", List(
              List(
                Aspect.Advice.byValue("key", key),
                Aspect.Advice.byValue("existing", existing),
              ),
              List(
                Aspect.Advice.byValue("newValue", newValue),
                Aspect.Advice.byValue("expiresIn", expiresIn),
              ),
            ), Aspect.Advice("updateExisting", af.updateExisting(key, existing)(newValue, expiresIn)))

          override def deleteExisting(key: K, existing: V): Aspect.Weave[F, TraceableValue, TraceableValue, Unit] =
            Aspect.Weave("RedisCache", List(List(
              Aspect.Advice.byValue("key", key),
              Aspect.Advice.byValue("existing", existing),
            )), Aspect.Advice("deleteExisting", af.deleteExisting(key, existing)))

      override def mapK[F[_], G[_]](af: RedisCache[F, K, V])(fk: F ~> G): RedisCache[G, K, V] =
        new RedisCacheMapKd(af, fk)

  private class RedisCacheMapKd[F[_], G[_], K, V](af: RedisCache[F, K, V],
                                                  fk: F ~> G) extends RedisCache[G, K, V]:
    override def get(key: K): G[Option[V]] =
      fk(af.get(key))

    override def set(key: K, value: V, setArgs: effects.SetArgs): G[Boolean] =
      fk(af.set(key, value, setArgs))

    override def del(key: K): G[Long] =
      fk(af.del(key))

    override def updateExisting(key: K, existing: V)
                               (newValue: V, expiresIn: FiniteDuration): G[Unit] =
      fk(af.updateExisting(key, existing)(newValue, expiresIn))

    override def deleteExisting(key: K, existing: V): G[Unit] =
      fk(af.deleteExisting(key, existing))


}
