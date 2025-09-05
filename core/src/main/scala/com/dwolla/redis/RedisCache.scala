package com.dwolla.redis

import cats.*
import cats.effect.{Trace as _, *}
import cats.syntax.all.*
import cats.effect.syntax.all.*
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

  // TODO remove this and figure out how to get the FunctorK from cats-tagless to work in LocalCache.apply
  final def mapK[G[_]](fk: F ~> G): RedisCache[G, K, V] = new RedisCache[G, K, V] {
    override def get(key: K): G[Option[V]] =
      fk(outer.get(key))

    override def set(key: K, value: V, setArgs: SetArgs): G[Boolean] =
      fk(outer.set(key, value, setArgs))

    override def del(key: K): G[Long] =
      fk(outer.del(key))

    override def updateExisting(key: K, existing: V)
                               (newValue: V, expiresIn: FiniteDuration): G[Unit] =
      fk(outer.updateExisting(key, existing)(newValue, expiresIn))

    override def deleteExisting(key: K, existing: V): G[Unit] =
      fk(outer.deleteExisting(key, existing))
  }

}

object RedisCache extends RedisCachePlatform {
  private[redis] type LimitedRedisCache[F[_], K, V] =
    Getter[F, K, V] & Setter[F, K, V] & KeyCommands[F, K] & HighLevelTx[F] & Watcher[F, K]

  case class UpdateOrDeleteExistingRedisValueException[K, V](key: K, expected: V, obtained: Option[V])
    extends RuntimeException(s"Cache contained unexpected value: expected «$expected» but found «$obtained»")
      with NoStackTrace

  def apply[F[_] : Temporal : Trace, K: TraceableValue, V : TraceableValue](instance: LimitedRedisCache[F, K, V],
                                                                            transactionalRetryPolicy: F ~> F): RedisCache[F, K, V] = {
    val core = RedisCore(instance)
    val tx = TransactionalRedisCommandsAlgebra(core, instance, transactionalRetryPolicy)
    new RedisCacheImpl(core, tx)
  }

  private[redis] class RedisCacheImpl[F[_], K, V](core: RedisCore[F, K, V],
                                                  tx: TransactionalRedisCommandsAlgebra[F, K, V]) extends RedisCache[F, K, V] {
    override def get(key: K): F[Option[V]] =
      core.get(key)

    override def set(key: K, value: V, setArgs: SetArgs): F[Boolean] =
      core.set(key, value, setArgs)

    override def del(key: K): F[Long] =
      core.del(key)

    override def updateExisting(key: K, existing: V)
                               (newValue: V, expiresIn: FiniteDuration): F[Unit] =
      tx.updateExisting(key, existing)(newValue, expiresIn)

    override def deleteExisting(key: K, existing: V): F[Unit] =
      tx.deleteExisting(key, existing)

  }
}

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

trait TransactionalRedisCommandsAlgebra[F[_], K, V] {
  def updateExisting(key: K, existing: V)
                    (newValue: V, expiresIn: FiniteDuration): F[Unit]

  def deleteExisting(key: K, existing: V): F[Unit]
}

object TransactionalRedisCommandsAlgebra {
  def apply[F[_] : MonadCancelThrow : Trace, K: TraceableValue, V: TraceableValue](coreCommands: RedisCore[F, K, V],
                                                                                   txCommands: HighLevelTx[F] & Watcher[F, K],
                                                                                   transactionalRetryPolicy: F ~> F,
                                                                                  ): TransactionalRedisCommandsAlgebra[F, K, V] = new TransactionalRedisCommandsAlgebraImpl(coreCommands, txCommands, transactionalRetryPolicy)

  private class TransactionalRedisCommandsAlgebraImpl[F[_] : MonadCancelThrow : Trace, K: TraceableValue, V: TraceableValue](coreCommands: RedisCore[F, K, V],
                                                                                                                             txCommands: HighLevelTx[F] & Watcher[F, K],
                                                                                                                             transactionalRetryPolicy: F ~> F,
                                                                                                                            ) extends TransactionalRedisCommandsAlgebra[F, K, V] {
    private def executeIfValueIsUnchanged(key: K, existing: V)
                                         (operations: List[F[Unit]]): F[Unit] = transactionalRetryPolicy {
      txCommands.watch(key)
        .bracket { _ =>
          for {
            currentValue <- coreCommands.get(key)
            _ <- currentValue.traverse_(cv => Trace[F].put("currentValue" -> cv))
            _ <- UpdateOrDeleteExistingRedisValueException(key, existing, currentValue).raiseError.unlessA(currentValue.contains(existing))
            _ <- txCommands.transact_(operations)
          } yield ()
        }(_ => txCommands.unwatch)
    }

    def updateExisting(key: K, existing: V)
                      (newValue: V, expiresIn: FiniteDuration): F[Unit] =
      Trace[F].span("TransactionalRedisCommandsAlgebra.updateExisting") {
        val setArgs = SetArgs(SetArg.Ttl.Px(expiresIn))
        for {
          _ <- Trace[F].put("key" -> key, "existing" -> existing, "newValue" -> newValue, "setArgs" -> setArgs)
          _ <- executeIfValueIsUnchanged(key, existing)(List(coreCommands.set(key, newValue, setArgs).void))
        } yield ()
      }

    def deleteExisting(key: K, existing: V): F[Unit] =
      Trace[F].span("TransactionalRedisCommandsAlgebra.deleteExisting") {
        for {
          _ <- Trace[F].put("key" -> key, "existing" -> existing)
          _ <- executeIfValueIsUnchanged(key, existing)(List(coreCommands.del(key).void))
        } yield ()
      }
  }
}
