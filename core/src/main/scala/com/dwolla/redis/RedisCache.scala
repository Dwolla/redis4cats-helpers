package com.dwolla.redis

import cats.*
import cats.effect.{Trace as _, *}
import cats.syntax.all.*
import com.dwolla.redis.RedisCache.UpdateOrDeleteExistingRedisValueException
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.algebra.*
import dev.profunktor.redis4cats.effects.*
import natchez.*

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

/**
 * @tparam F the effect type in which to operate
 * @tparam K the type of Key handled by this instance
 * @tparam V the type of Value handled by this instance
 */
trait RedisCache[F[_], K, V] { outer =>
  def get(key: K): F[Option[V]]

  def set(key: K, value: V, setArgs: SetArgs): F[Boolean]

  def del(key: K): F[Long]

  def updateExisting(key: K, existing: V)
                    (newValue: V, expiresIn: FiniteDuration): F[Unit]

  def deleteExisting(key: K, existing: V): F[Unit]

  final def contramapKey[K2](g: K2 => K): RedisCache[F, K2, V] = new RedisCache[F, K2, V] {
    override def get(key: K2): F[Option[V]] =
      outer.get(g(key))

    override def set(key: K2, value: V, setArgs: SetArgs): F[Boolean] =
      outer.set(g(key), value, setArgs)

    override def updateExisting(key: K2, existing: V)(value: V, expiresIn: FiniteDuration): F[Unit] =
      outer.updateExisting(g(key), existing)(value, expiresIn)

    override def del(key: K2): F[Long] =
      outer.del(g(key))

    override def deleteExisting(key: K2, existing: V): F[Unit] =
      outer.deleteExisting(g(key), existing)
  }

  final def imapValue[V2](f: V => Either[Throwable, V2])
                         (g: V2 => V)
                         (implicit F: MonadThrow[F]): RedisCache[F, K, V2] = new RedisCache[F, K, V2] {
    override def get(key: K): F[Option[V2]] =
      outer.get(key)
        .flatMap {
          _
            .traverse(f)
            .liftTo[F]
        }

    override def set(key: K, value: V2, setArgs: SetArgs): F[Boolean] =
      outer.set(key, g(value), setArgs)

    override def updateExisting(key: K, existing: V2)(value: V2, expiresIn: FiniteDuration): F[Unit] =
      outer.updateExisting(key, g(existing))(g(value), expiresIn)

    override def del(key: K): F[Long] =
      outer.del(key)

    override def deleteExisting(key: K, existing: V2): F[Unit] =
      outer.deleteExisting(key, g(existing))
  }
}

object RedisCache extends RedisCachePlatform {
  private type LimitedRedisCache[F[_], K, V] =
    Getter[F, K, V] & Setter[F, K, V] & KeyCommands[F, K] & HighLevelTx[F] & Watcher[F, K]

  case class UpdateOrDeleteExistingRedisValueException[K, V](key: K, expected: V, obtained: Option[V])
    extends RuntimeException(s"Cache contained unexpected value: expected «$expected» but found «$obtained»")
      with NoStackTrace

  private class DelegatingRedisCacheImpl[F[_] : Temporal : Trace, K: TraceableValue, V: TraceableValue](instance: LimitedRedisCache[F, K, V],
                                                                                                        transactionalRetryPolicy: F ~> F)
    extends TransactionalRedisCommandsAlgebra[F, K, V](instance, transactionalRetryPolicy) {

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

  def apply[F[_] : Temporal : Trace, K: TraceableValue, V : TraceableValue](instance: LimitedRedisCache[F, K, V],
                                                                            transactionalRetryPolicy: F ~> F): RedisCache[F, K, V] =
    new DelegatingRedisCacheImpl(instance, transactionalRetryPolicy)
}

abstract class TransactionalRedisCommandsAlgebra[F[_] : MonadThrow : Trace, K: TraceableValue, V: TraceableValue](instance: HighLevelTx[F] & Watcher[F, K],
                                                                                                                  transactionalRetryPolicy: F ~> F,
                                                                                                                 ) extends RedisCache[F, K, V] {
  private def raiseUpdateOrDeleteExistingRedisValueException(key: K, existing: V, currentValue: Option[V]): F[Unit] =
    instance.unwatch >> UpdateOrDeleteExistingRedisValueException(key, existing, currentValue).raiseError

  private def executeIfValueIsUnchanged(key: K, existing: V)
                                       (operations: List[F[Unit]]): F[Unit] = transactionalRetryPolicy {
    for {
      _ <- instance.watch(key)
      currentValue <- get(key)
      _ <- currentValue.traverse_(cv => Trace[F].put("currentValue" -> cv))
      _ <- raiseUpdateOrDeleteExistingRedisValueException(key, existing, currentValue).unlessA(currentValue.contains(existing))
      _ <- instance.transact_(operations)
    } yield ()
  }

  def updateExisting(key: K, existing: V)
                    (newValue: V, expiresIn: FiniteDuration): F[Unit] =
    Trace[F].span("TransactionalRedisCommandsAlgebra.updateExisting") {
      val setArgs = SetArgs(SetArg.Ttl.Px(expiresIn))
      for {
        _ <- Trace[F].put("key" -> key, "existing" -> existing, "newValue" -> newValue, "setArgs" -> setArgs)
        _ <- executeIfValueIsUnchanged(key, existing)(List(set(key, newValue, setArgs).void))
      } yield ()
    }

  def deleteExisting(key: K, existing: V): F[Unit] =
    Trace[F].span("TransactionalRedisCommandsAlgebra.deleteExisting") {
      for {
        _ <- Trace[F].put("key" -> key, "existing" -> existing)
        _ <- executeIfValueIsUnchanged(key, existing)(List(del(key).void))
      } yield ()
    }

}
