package com.dwolla.redis

import cats.*
import cats.data.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.effect.{Trace as _, *}
import cats.syntax.all.*
import com.dwolla.redis.LocalCache.*
import com.dwolla.redis.TraceableValueInstances.*
import dev.profunktor.redis4cats.algebra.{HighLevelTx, Watcher}
import dev.profunktor.redis4cats.effects.{SetArg, SetArgs}
import dev.profunktor.redis4cats.tx.{TransactionDiscarded, TxStore}
import io.circe.*
import io.circe.literal.*
import io.circe.syntax.*
import natchez.*
import retry.*
import retry.syntax.all.*

import java.time.Instant
import scala.collection.immutable.*
import scala.concurrent.duration.*
import scala.math.Ordered.orderingToOrdered

/**
 * A specialized extension of [[RedisCache]] that retains and exposes the full cache state as a map,
 * useful for testing.
 *
 * This type allows us to abstract away the details of [[LocalCache]], which are complicated and
 * normally unnecessary when using these classes to write tests.
 *
 * @tparam F the effect type used for operations
 * @tparam K the key type for the cache
 * @tparam V the value type for the cache
 */
trait CacheState[F[_], K, V] extends RedisCache[F, K, V] { self =>
  def getCacheState: F[Map[K, (V, Option[Instant])]]

  def mapK[G[_]](fk: F ~> G): CacheState[G, K, V] = new CacheState[G, K, V] {
    override def getCacheState: G[Map[K, (V, Option[Instant])]] = fk(self.getCacheState)
    override def get(key: K): G[Option[V]] = fk(self.get(key))
    override def set(key: K, value: V, setArgs: SetArgs): G[Boolean] = fk(self.set(key, value, setArgs))
    override def updateExisting(key: K, existing: V)(value: V, expiresIn: FiniteDuration): G[Unit] = fk(self.updateExisting(key, existing)(value, expiresIn))
    override def del(key: K): G[Long] = fk(self.del(key))
    override def deleteExisting(key: K, existing: V): G[Unit] = fk(self.deleteExisting(key, existing))
  }
}

/**
 * A local in-memory cache implementation of the `RedisCache` trait that ultimately operates
 * using an `AtomicCell`, supporting custom expiration times and race condition behaviors.
 *
 * This class implements `CacheState[StateT[F, Map[K, (V, Option[Instant])], *], K, V]` and
 * is then converted to an implementation in `F[_]` in its companion object. This allows us
 * to sequence a series of `StateT[F, Map[K, (V, Option[Instant])], Unit]` operations together
 * in our [[LocalRedisTransactions]], allowing them to operate as a fused
 * `Map[K, (V, Option[Instant])] => F[(Map[K, (V, Option[Instant])], Unit)]` passed to
 * [[AtomicCell.evalModify]]. Without this indirection, any `set` operations executed inside
 * the `evalModify` cause deadlocks, preventing the successful implementation of [[updateExisting]].
 *
 * @param watches tracks watches key/values during transactional operations
 * @param verboseMode when true, emit quite a bit of detail about operations to the console. otherwise, be quiet
 * @tparam F the effect type in which to operate
 * @tparam K the type of the key in the cache
 * @tparam V the type of the value in the cache
 */
class LocalCache[
  F[_] : MonadCancelThrow : Clock : Console : Trace,
  K: KeyEncoder : Ordering : TraceableValue,
  V : Encoder : Eq : TraceableValue
] private(watches: Ref[F, SortedMap[K, V]],
          verboseMode: Boolean,
          transactionalRetryPolicy: StateT[F, Map[K, (V, Option[Instant])], *] ~> StateT[F, Map[K, (V, Option[Instant])], *],
         )
         (implicit Mk: Ref.Make[StateT[F, Map[K, (V, Option[Instant])], *]])
  extends TransactionalRedisCommandsAlgebra[StateT[F, Map[K, (V, Option[Instant])], *], K, V](LocalRedisTransactions(watches, verboseMode), transactionalRetryPolicy)
    with CacheState[StateT[F, Map[K, (V, Option[Instant])], *], K, V] {

  override def getCacheState: StateT[F, Map[K, (V, Option[Instant])], Map[K, (V, Option[Instant])]] = StateT.get

  private type StF[a] = StateT[F, Map[K, (V, Option[Instant])], a]

  override def get(key: K): StateT[F, Map[K, (V, Option[Instant])], Option[V]] = Trace[StF].span("LocalCache.get") {
    Trace[StF].put("key" -> key) >>
      StateT { (cache: Map[K, (V, Option[Instant])]) =>
        Clock[F].realTimeInstant
          .flatTap(now => Console[F].println(s"get($key) using $cache at $now").whenA(verboseMode))
          .flatMap { now =>
            getFromCacheWithTtl(cache, now, key) match {
              case CacheGet.Hit(v, ttl) =>
                Trace[F].log(s"found live value: ttl = $ttl, now = $now").as(cache -> v.some)

              case CacheGet.Miss =>
                (cache -> none[V]).pure[F]

              case CacheGet.Expired(_, _) =>
                Trace[F].log("found expired value; removing it and returning None").as((cache - key) -> none[V])

            }
          }.guaranteeCase {
            case Outcome.Succeeded(fa) =>
              for {
                (state, output) <- fa
                _ <- Trace[F].put("returnValue" -> output)
                _ <- Console[F].println(s"get($key) returning ${(state, output)}").whenA(verboseMode)
              } yield ()
            case Outcome.Errored(t) => Console[F].println(s"get($key) failed with $t").whenA(verboseMode)
            case Outcome.Canceled() => Console[F].println(s"get($key) canceled").whenA(verboseMode)
          }
      }
  }

  private def ttlToInstant(now: Instant, maybeTtl: Option[SetArg.Ttl]): Option[Instant] =
    maybeTtl.getOrElse(SetArg.Ttl.Keep) match {
      case SetArg.Ttl.Px(millis) =>
        now.plusMillis(millis.toMillis).some

      case SetArg.Ttl.Ex(seconds) =>
        now.plusSeconds(seconds.toSeconds).some

      case SetArg.Ttl.Keep =>
        None
    }

  override def set(key: K, value: V, setArgs: SetArgs): StateT[F, Map[K, (V, Option[Instant])], Boolean] =
    Trace[StF].span("LocalCache.set") {
      Trace[StF].put("key" -> key, "value" -> value, "setArgs" -> setArgs) >>
        StateT { (cache: Map[K, (V, Option[Instant])]) =>
          Clock[F].realTimeInstant
            .flatTap(now => Console[F].println(s"set($key, $value, $setArgs) using $cache at $now").whenA(verboseMode))
            .flatMap { now =>
              setArgs match {
                case SetArgs(Some(SetArg.Existence.Nx), ttl) =>
                  if (getFromCacheWithTtl(cache, now, key).isDefined) Trace[F].log("⚠️ SetNx but cache contains key, not updating").as(cache -> false)
                  else Trace[F].log("✅ SetNx and cache does not contain key, updating").as(cache.updated(key, value -> ttlToInstant(now, ttl)) -> true)

                case SetArgs(Some(SetArg.Existence.Xx), ttl) =>
                  if (getFromCacheWithTtl(cache, now, key).isEmpty) Trace[F].log("⚠️ SetXx but cache does not contain key, not updating").as(cache -> false)
                  else Trace[F].log("✅ SetXx and cache contains key, updating").as(cache.updated(key, value -> ttlToInstant(now, ttl)) -> true)

                case SetArgs(None, ttl) =>
                  (cache.updated(key, value -> ttlToInstant(now, ttl)) -> true).pure[F]
              }
            }.guaranteeCase {
              case Outcome.Succeeded(fa) =>
                for {
                  (state, output) <- fa
                  now <- Clock[F].realTimeInstant
                  _ <- Trace[F].put("returnValue" -> output, "state" -> state, "now" -> now)
                  _ <- Trace[F].log("Set Failed").unlessA(output)
                  _ <- Console[F].println(s"set($key, $value, $setArgs) returning $output with new cache state $state").whenA(verboseMode)
                } yield ()
              case Outcome.Errored(t) => Console[F].println(s"set($key, $value, $setArgs) failed with $t").whenA(verboseMode)
              case Outcome.Canceled() => Console[F].println(s"set($key, $value, $setArgs) canceled").whenA(verboseMode)
            }
        }
    }

  override def del(key: K): StateT[F, Map[K, (V, Option[Instant])], Long] = Trace[StF].span("LocalCache.del") {
    Trace[StF].put("key" -> key) >>
      StateT { cache =>
        Clock[F].realTimeInstant
          .flatTap(now => Console[F].println(s"del($key) using $cache at $now").whenA(verboseMode))
          .as((cache - key) -> (if (cache.contains(key)) 1L else 0L))
          .guaranteeCase {
            case Outcome.Succeeded(fa) =>
              for {
                (state, output) <- fa
                _ <- Trace[F].put("returnValue" -> output)
                _ <- Console[F].println(s"del($key) returning ${(state, output)}").whenA(verboseMode)
              } yield ()
            case Outcome.Errored(t) => Console[F].println(s"del($key) failed with $t").whenA(verboseMode)
            case Outcome.Canceled() => Console[F].println(s"del($key) canceled").whenA(verboseMode)
          }
      }
  }

}

/**
 * Generically applies the configuration options to the input effects so the
 * application logic doesn't have to be present in every method of the algebras
 * being transformed.
 *
 * @param explodeWhen         A partial function specifying conditions under which specific cache operations
 *                            should throw exceptions. Defaults to an empty partial function.
 * @param sleepAfterWhen      A partial function specifying conditions under which specific cache operations
 *                            should introduce delays. Defaults to an empty partial function.
 */
private class ApplyCacheOperationModifications[
  F[_] : Temporal : Trace,
  K: TraceableValue,
  V: TraceableValue,
](explodeWhen: PartialFunction[CacheOperation[K, V], Throwable] = PartialFunction.empty,
  sleepAfterWhen: PartialFunction[CacheOperation[K, V], FiniteDuration] = PartialFunction.empty,
 )
  extends (F ~> Kleisli[F, CacheOperation[K, V], *]) {

  override def apply[A](fa: F[A]): Kleisli[F, CacheOperation[K, V], A] = {
    val explodeOrExecute: Kleisli[F, CacheOperation[K, V], A] =
      Kleisli { op =>
        Trace[F].span(op.getClass.getSimpleName) {
          Trace[F].put("op" -> op) >>
            explodeWhen.lift(op)
              .map(_.raiseError[F, A])
              .getOrElse(fa)
        }
      }

    val maybeSleepAfter: Kleisli[F, CacheOperation[K, V], Unit] =
      Kleisli { op =>
        sleepAfterWhen
          .lift(op)
          .traverse_ { d =>
            Trace[F].span("sleeping after operation") {
              Trace[F].put("operation" -> op, "sleep_duration" -> d) >> Temporal[F].sleep(d)
            }
          }
      }

    explodeOrExecute.flatTap(_ => maybeSleepAfter)
  }
}

/**
 * Transforms `StateT[F, Map[K, (V, Option[Instant])], A]` to `F[A]` by using the
 * `evalModify` method of the given [[AtomicCell]]. This materializes the state
 * transition represented by the input using the [[AtomicCell]] for concurrent access.
 */
private class MaterializeStateTransitionsUsingAtomicCell[F[_] : Temporal : Trace, K: KeyEncoder, V: Encoder](cell: AtomicCell[F, Map[K, (V, Option[Instant])]])
  extends (StateT[F, Map[K, (V, Option[Instant])], *] ~> F) {

  override def apply[A](fa: StateT[F, Map[K, (V, Option[Instant])], A]): F[A] =
    Trace[F].span("MaterializeStateTransitionsUsingAtomicCell.apply") {
      for {
        preState <- cell.get
        out <- cell.evalModify(fa.run)
        postState <- cell.get
        _ <- Trace[F].put("preState" -> preState, "postState" -> postState)
      } yield out
    }
}

object LocalCache {
  /**
   * Creates and initializes a new instance of [[CacheState]] with provided configurations and dependencies.
   *
   * This initializes a [[LocalCache]] instance, which is a `CacheState[StateT[F, Map[K, (V, Option[Instant])], *], K, V]`.
   * The `StateT` effect allows us to logically call [[LocalCache.set]] inside [[LocalCache.updateExisting]], which would
   * normally cause a deadlock, since [[LocalCache.updateExisting]] obtains exclusive access to update the underlying
   * `AtomicCell[F, Map[K, (V, Option[Instant])]]`. When the `set` operation is a `StateT`, it can be given the current
   * state and allowed to compute the next state, which is then used by [[AtomicCell.evalModify]] to update the cell.
   *
   * @param explodeWhen         A partial function specifying conditions under which specific cache operations
   *                            should throw exceptions. Defaults to an empty partial function.
   * @param sleepAfterWhen      A partial function specifying conditions under which specific cache operations
   *                            should introduce delays. Defaults to an empty partial function.
   * @param verboseMode         A Boolean flag to enable or disable verbose logging. Defaults to `false`.
   * @param transactionalRetryPolicy a [[FunctionK]] that may retry the effect when [[TransactionDiscarded]] is raised
   * @tparam F An effect type that supports `Console`, `Async`, and `Trace` type classes.
   * @tparam K The type of keys in the cache, which must have instances for `KeyEncoder`,
   *           `Ordering`, and `TraceableValue`.
   * @tparam V The type of values in the cache, which must have instances for `Empty`, `Encoder`,
   *           `Eq`, and `TraceableValue`.
   * @return An effect that, when evaluated, returns the initialized `CacheState` instance.
   */
  def apply[
    F[_] : Temporal : Console : Trace,
    K: KeyEncoder : Ordering : TraceableValue,
    V : Encoder : Eq : TraceableValue,
  ](explodeWhen: PartialFunction[CacheOperation[K, V], Throwable],
    sleepAfterWhen: PartialFunction[CacheOperation[K, V], FiniteDuration],
    verboseMode: Boolean,
    transactionalRetryPolicy: StateT[F, Map[K, (V, Option[Instant])], *] ~> StateT[F, Map[K, (V, Option[Instant])], *],
   )
   (implicit Mk: Ref.Make[StateT[F, Map[K, (V, Option[Instant])], *]]): F[CacheState[F, K, V]] = {
    (AtomicCell[F].of(Map.empty[K, (V, Option[Instant])]), Ref[F].of(SortedMap.empty[K, V]))
      .mapN { (cell, watches) =>

        val impl =
          new LocalCache[F, K, V](watches, verboseMode, transactionalRetryPolicy)
            .mapK(new MaterializeStateTransitionsUsingAtomicCell(cell))
            .mapK(new ApplyCacheOperationModifications(explodeWhen, sleepAfterWhen))

        // TODO it feels like with more development effort, this could be done generically with something from cats-tagless
        new CacheState[F, K, V] {
          override def getCacheState: F[Map[K, (V, Option[Instant])]] =
            impl.getCacheState.run(CacheOperation.GetCacheState)
          override def get(key: K): F[Option[V]] =
            impl.get(key).run(CacheOperation.Get(key))
          override def set(key: K, value: V, setArgs: SetArgs): F[Boolean] =
            impl.set(key, value, setArgs)
              .run(CacheOperation.Set(key, value, setArgs))
          override def updateExisting(key: K, existing: V)(value: V, expiresIn: FiniteDuration): F[Unit] =
            impl.updateExisting(key, existing)(value, expiresIn)
              .run(CacheOperation.UpdateExisting(key, existing, value, expiresIn))
          override def del(key: K): F[Long] =
            impl.del(key).run(CacheOperation.Del(key))
          override def deleteExisting(key: K, existing: V): F[Unit] =
            impl.deleteExisting(key, existing).run(CacheOperation.DeleteExisting(key, existing))
        }
      }
  }

  def getFromCacheWithTtl[K, V](cache: Map[K, (V, Option[Instant])],
                                now: Instant,
                                key: K): CacheGet[V] =
    cache.get(key) match {
      case Some((v, ttl)) if ttl.exists(now < _) =>
        CacheGet.hit(v, ttl)

      case Some((v, None)) =>
        CacheGet.hit(v, None)

      case Some((v, Some(ttl))) =>
        CacheGet.expired(v, ttl)

      case None =>
        CacheGet.miss
    }

  private[redis] implicit def stfSleep[F[_] : Temporal, S]: retry.Sleep[StateT[F, S, *]] =
    delay => StateT.liftF(Temporal[F].sleep(delay))

}

class TransactionalRetryPolicy[F[_]: MonadThrow : Sleep : Trace] extends (F ~> F) {
  private implicit val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Long].contramap(_.toMillis)

  private implicit val retryDetailsEncoder: Encoder[RetryDetails] = Encoder.instance { rd =>
    json"""{
            "retriesSoFar": ${rd.retriesSoFar},
            "cumulativeDelayMs": ${rd.cumulativeDelay},
            "givingUp": ${rd.givingUp},
            "upcomingDelay":${rd.upcomingDelay}
          }"""
  }

  private def addRetryDetailsToTrace(ex: Throwable, rd: RetryDetails): F[Unit] =
    Trace[F].attachError(ex, "retryDetails" -> rd)

  override def apply[A](fa: F[A]): F[A] =
    fa.retryingOnSomeErrors(
      _.isInstanceOf[TransactionDiscarded.type].pure[F],
      RetryPolicies.fullJitter(10.millis).join(RetryPolicies.limitRetries(3)),
      addRetryDetailsToTrace
    )
}

/**
 * An algebraic data type representing the potential results from a cache GET operation.
 */
sealed trait CacheGet[+V] {
  def isDefined: Boolean
  def isEmpty: Boolean
}
object CacheGet {
  def hit[V](v: V, ttl: Option[Instant]): CacheGet[V] = Hit(v, ttl)
  def miss: CacheGet[Nothing] = Miss
  def expired[V](v: V, ttl: Instant): CacheGet[V] = Expired(v, ttl)

  case class Hit[V](v: V, ttl: Option[Instant]) extends CacheGet[V] {
    override def isDefined: Boolean = true
    override def isEmpty: Boolean = false
  }
  case object Miss extends CacheGet[Nothing] {
    override def isDefined: Boolean = false
    override def isEmpty: Boolean = true
  }
  case class Expired[V](v: V, ttl: Instant) extends CacheGet[V] {
    override def isDefined: Boolean = false
    override def isEmpty: Boolean = true
  }
}

/**
 * An algebraic data type representing the various operations on [[RedisCache]].
 */
sealed trait CacheOperation[+K, +V]
object CacheOperation {
  case class Get[K](key: K) extends CacheOperation[K, Nothing]
  case class Set[K, V](key: K, value: V, setArgs: SetArgs) extends CacheOperation[K, V]
  case class UpdateExisting[K, V](key: K, existing: V, newValue: V, ttl: FiniteDuration) extends CacheOperation[K, V]
  case class Del[K](key: K) extends CacheOperation[K, Nothing]
  case class DeleteExisting[K, V](key: K, existing: V) extends CacheOperation[K, V]
  case object GetCacheState extends CacheOperation[Nothing, Nothing]

  private def stringFromTraceValue[A: TraceableValue](a: A): String = TraceableValue[A].toTraceValue(a) match {
    case TraceValue.StringValue(value) => value
    case TraceValue.BooleanValue(value) => value.toString
    case TraceValue.NumberValue(value) => value.toString
  }

  implicit def traceableValue[K: TraceableValue, V: TraceableValue]: TraceableValue[CacheOperation[K, V]] = {
    case Get(key) =>
      s"Get(${stringFromTraceValue[K](key)})"
    case Set(key, value, setArgs) =>
      s"Set(${stringFromTraceValue[K](key)}, ${stringFromTraceValue[V](value)}, $setArgs)"
    case UpdateExisting(key, existing, newValue, ttl) =>
      s"UpdateExisting(${stringFromTraceValue[K](key)}, ${stringFromTraceValue[V](existing)}, ${stringFromTraceValue[V](newValue)}, $ttl)"
    case DeleteExisting(key, existing) =>
      s"DeleteExisting(${stringFromTraceValue[K](key)}, ${stringFromTraceValue[V](existing)}"
    case Del(key) =>
      s"Del(${stringFromTraceValue[K](key)})"
    case GetCacheState =>
      "GetCacheState"
  }
}

/**
 * A local in-memory implementation of the redis4cats interfaces [[HighLevelTx]] with [[Watcher]].
 *
 * This is implemented in `StateT[F, Map[K, (V, Option[Instant])], *]` so that the list of `F[Unit]`s
 * passed to [[transact]] and [[transact_]] can be run inside the exclusive modification of an
 * [[AtomicCell]] without causing a deadlock.
 *
 * @tparam K Type of the key used in the cache
 * @tparam V Type of the value stored in the cache
 * @param watched The currently watched keys and their values when each watch was started. Passed as a constructor
 *                parameter because initializing the [[Ref]] is effectful.
 * @param verboseMode when true, emit quite a bit of detail about operations to the console. otherwise, be quiet
 */
class LocalRedisTransactions[F[_] : MonadCancelThrow : Clock : Console : Trace, K: Ordering : KeyEncoder, V: Eq : Encoder](watched: Ref[F, SortedMap[K, V]],
                                                                                                                           verboseMode: Boolean)
                                                                                                                          (implicit Mk: Ref.Make[StateT[F, Map[K, (V, Option[Instant])], *]])
  extends HighLevelTx[StateT[F, Map[K, (V, Option[Instant])], *]]
    with Watcher[StateT[F, Map[K, (V, Option[Instant])], *], K] {
  private type StF[a] = StateT[F, Map[K, (V, Option[Instant])], a]

  private def checkWatchesAndThenTransact[A](fa: StateT[F, Map[K, (V, Option[Instant])], A]): StateT[F, Map[K, (V, Option[Instant])], A] =
    Console[StF].println("entering LocalWatcher.checkWatchesAndThenTransact").whenA(verboseMode) >>
      Trace[StF].span("LocalWatcher.checkWatchesAndThenTransact") {
        StateT.inspectF { (cacheState: Map[K, (V, Option[Instant])]) =>
          for {
            watchedState <- watched.get
            _ <- {
              TransactionDiscarded.raiseError.unlessA {
                watchedState.forall { case (k, v) =>
                  cacheState.get(k).exists(_._1 === v)
                }
              }
            }
          } yield ()
        } >> fa
      }
        .guarantee(unwatch)
        .guarantee(Console[StF].println("leaving LocalWatcher.checkWatchesAndThenTransact").whenA(verboseMode))

  override def transact[A](fs: TxStore[StateT[F, Map[K, (V, Option[Instant])], *], String, A] => List[StateT[F, Map[K, (V, Option[Instant])], Unit]]): StateT[F, Map[K, (V, Option[Instant])], Map[String, A]] =
    checkWatchesAndThenTransact {
      Trace[StF].span("LocalWatcher.transact") {
        for {
          store <- Ref.of[StF, Map[String, A]](Map.empty).map { ref =>
            new TxStore[StF, String, A] {
              def get: StF[Map[String, A]] = ref.get
              def set(key: String)(v: A): StF[Unit] = ref.update(_.updated(key, v))
            }
          }
          _ <- fs(store).sequence_
          map <- store.get
        } yield map
      }
    }

  override def transact_(fs: List[StateT[F, Map[K, (V, Option[Instant])], Unit]]): StateT[F, Map[K, (V, Option[Instant])], Unit] =
    checkWatchesAndThenTransact(Trace[StF].span("LocalWatcher.transact_")(fs.sequence_))

  override def watch(keys: K*): StateT[F, Map[K, (V, Option[Instant])], Unit] = Trace[StF].span("LocalWatcher.watch") {
    StateT.inspectF { (cacheState: Map[K, (V, Option[Instant])]) =>
      for {
        now <- Clock[F].realTimeInstant
        watchState <- watched.updateAndGet { existingWatches =>
          val newWatches =
            keys.filterNot(existingWatches.keySet) // don't update existing watches
              .foldLeft(Map.empty[K, V]) { (acc, key) =>
                getFromCacheWithTtl(cacheState, now, key) match {
                  case CacheGet.Hit(v, _) => acc.updated(key, v)
                  case _ => acc
                }
              }

          existingWatches ++ newWatches
        }
        _ <- Trace[F].put("updated_watch_state" -> watchState.asJson)
      } yield ()
    }
  }

  override def unwatch: StateT[F, Map[K, (V, Option[Instant])], Unit] =
    StateT.liftF {
      Console[F].println("entering LocalWatcher.unwatch").whenA(verboseMode) >> Trace[F].span("LocalWatcher.unwatch") {
        watched.set(SortedMap.empty)
      }.guarantee(Console[F].println("leaving LocalWatcher.unwatch").whenA(verboseMode))
    }
}

object LocalRedisTransactions {
  def apply[F[_] : MonadCancelThrow : Clock : Console : Trace, K: Ordering : KeyEncoder, V: Eq : Encoder](watched: Ref[F, SortedMap[K, V]],
                                                                                                          verboseMode: Boolean)
                                                                                                         (implicit Mk: Ref.Make[StateT[F, Map[K, (V, Option[Instant])], *]]): HighLevelTx[StateT[F, Map[K, (V, Option[Instant])], *]] & Watcher[StateT[F, Map[K, (V, Option[Instant])], *], K] =
    new LocalRedisTransactions(watched, verboseMode)
}

object LocalCacheBuilder {
  def apply[F[_] : Temporal : Trace, K, V] = new LocalCacheBuilder[F, K, V](
    explodeWhen = PartialFunction.empty,
    sleepAfterWhen = PartialFunction.empty,
    verboseMode = false,
    transactionalRetryPolicy = new TransactionalRetryPolicy[StateT[F, Map[K, (V, Option[Instant])], *]]
  )
}

class LocalCacheBuilder[F[_], K, V] private(explodeWhen: PartialFunction[CacheOperation[K, V], Throwable],
                                            sleepAfterWhen: PartialFunction[CacheOperation[K, V], FiniteDuration],
                                            verboseMode: Boolean,
                                            transactionalRetryPolicy: StateT[F, Map[K, (V, Option[Instant])], *] ~> StateT[F, Map[K, (V, Option[Instant])], *]) {
  def explodingWhen(pf: PartialFunction[CacheOperation[K, V], Throwable]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(pf, sleepAfterWhen, verboseMode, transactionalRetryPolicy)

  def sleepingAfter(pf: PartialFunction[CacheOperation[K, V], FiniteDuration]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, pf, verboseMode, transactionalRetryPolicy)

  def verbose(): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode = true, transactionalRetryPolicy)

  def withVerbose(b: Boolean): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode = b, transactionalRetryPolicy)

  def withTransactionalRetryPolicy(trp: StateT[F, Map[K, (V, Option[Instant])], *] ~> StateT[F, Map[K, (V, Option[Instant])], *]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode, trp)

  def build()
           (implicit
            F: Temporal[F],
            C: Console[F],
            T: Trace[F],
            KE: KeyEncoder[K],
            O: Ordering[K],
            KTV: TraceableValue[K],
            VE: Encoder[V],
            VEq: Eq[V],
            VTV: TraceableValue[V],
            Mk: Ref.Make[StateT[F, Map[K, (V, Option[Instant])], *]]): F[CacheState[F, K, V]] =
    LocalCache(explodeWhen, sleepAfterWhen, verboseMode, transactionalRetryPolicy)
}
