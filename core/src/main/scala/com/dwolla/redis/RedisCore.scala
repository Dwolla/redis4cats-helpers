package com.dwolla.redis

import cats.*
import cats.effect.{Trace as _, *}
import cats.syntax.all.*
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.algebra.*
import dev.profunktor.redis4cats.effects.*
import natchez.*


trait RedisCore[F[_], K, V] { outer =>
  def get(key: K): F[Option[V]]

  def set(key: K, value: V, setArgs: SetArgs): F[Boolean]

  def del(key: K): F[Long]

  final def mapK[G[_]](fk: F ~> G): RedisCore[G, K, V] = new RedisCore[G, K, V] {
    override def get(key: K): G[Option[V]] = fk(outer.get(key))
    override def set(key: K, value: V, setArgs: SetArgs): G[Boolean] = fk(outer.set(key, value, setArgs))
    override def del(key: K): G[Long] = fk(outer.del(key))
  }
}

object RedisCore {
  def apply[F[_] : Temporal : Trace, K: TraceableValue, V : TraceableValue](instance: Getter[F, K, V] & Setter[F, K, V] & KeyCommands[F, K]): RedisCore[F, K, V] =
    new DelegatingRedisCacheImpl(instance)

  private class DelegatingRedisCacheImpl[F[_] : Temporal : Trace, K: TraceableValue, V: TraceableValue](instance: Getter[F, K, V] & Setter[F, K, V] & KeyCommands[F, K]) extends RedisCore[F, K, V] {

    override def get(key: K): F[Option[V]] = Trace[F].span("DelegatingRedisCacheImpl.get") {
      for {
        _ <- Trace[F].put("key" -> key)
        output <- instance.get(key)
        _ <- output.traverse_(o => Trace[F].put("output" -> o))
      } yield output
    }

    override def set(key: K, value: V, setArgs: SetArgs): F[Boolean] = Trace[F].span("DelegatingRedisCacheImpl.set") {
      for {
        _ <- Trace[F].put("key" -> key, "value" -> value, "setArgs" -> setArgs)
        isSuccess <- instance.set(key, value, setArgs)
        _ <- Trace[F].put("success" -> isSuccess)
      } yield isSuccess
    }

    override def del(key: K): F[Long] = Trace[F].span("DelegatingRedisCacheImpl.del") {
      for {
        _ <- Trace[F].put("key" -> key)
        recordsDeleted <- instance.del(key)
        _ <- Trace[F].put("recordsDeleted" -> recordsDeleted)
      } yield recordsDeleted
    }
  }
}
