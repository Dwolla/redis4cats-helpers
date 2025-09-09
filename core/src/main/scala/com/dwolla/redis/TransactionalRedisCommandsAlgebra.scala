package com.dwolla.redis

import cats.*
import cats.effect.syntax.all.*
import cats.effect.{Trace as _, *}
import cats.syntax.all.*
import com.dwolla.redis.RedisCache.UpdateOrDeleteExistingRedisValueException
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.algebra.*
import dev.profunktor.redis4cats.effects.*
import natchez.*

import scala.concurrent.duration.*

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
