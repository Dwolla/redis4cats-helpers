package com.dwolla.redis

import cats.*
import cats.data.{StateT, *}
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
import org.typelevel.keypool.KeyPool
import retry.*
import retry.syntax.all.*

import java.time.Instant
import scala.collection.immutable.*
import scala.concurrent.duration.*
import scala.math.Ordered.orderingToOrdered

trait LocalCachePool[F[_], K, V] {
  def cacheState: F[CacheState[K, V]]
  def pool: KeyPool[F, Unit, RedisCache[F, K, V]]
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
private class MaterializeStateTransitionsUsingAtomicCell[F[_] : Temporal : Trace, K: KeyEncoder, V: Encoder](cell: AtomicCell[F, CacheState[K, V]],
                                                                                                             watches: AtomicCell[F, Watches[K, V]])
  extends (StateT[F, InstanceState[K, V], *] ~> F) {

  override def apply[A](fa: StateT[F, InstanceState[K, V], A]): F[A] =
    Trace[F].span("MaterializeStateTransitionsUsingAtomicCell.apply") {
      cell.evalModify[A] { (preCacheState: CacheState[K, V]) =>
        watches.evalModify[(CacheState[K, V], A)] { (preWatches: Watches[K, V]) =>
          fa.run(InstanceState(preCacheState, preWatches))
            .flatTap { case (postInstanceState, _) =>
              Trace[F].put(
                "preCacheState" -> preCacheState,
                "preWatches" -> preWatches,
                "postCacheState" -> postInstanceState.cacheState,
                "postWatches" -> postInstanceState.watches,
              )
            }
            .map(InstanceState.forEvalModify)
        }
      }
    }
}

private class UnfocusCacheStateToInstanceState[F[_] : Functor, K, V] extends (StateT[F, CacheState[K, V], *] ~> StateT[F, InstanceState[K, V], *]) {
  override def apply[A](fa: StateT[F, CacheState[K, V], A]): StateT[F, InstanceState[K, V], A] =
    fa.transformS[InstanceState[K, V]](_.cacheState, _.withCacheState(_))
}

object LocalCache {
  type Watches[K, V] = SortedMap[K, V]
  type CacheState[K, V] = Map[K, (V, Option[Instant])]

  class InstanceState[K, V] private(val cacheState: CacheState[K, V],
                                    val watches: Watches[K, V]) {
    def withCacheState(cs: CacheState[K, V]) = new InstanceState(cs, watches)
    def withWatches(w: Watches[K, V]) = new InstanceState(cacheState, w)

    def forEvalModify[A](a: A): (Watches[K, V], (CacheState[K, V], A)) =
      watches -> (cacheState -> a)
  }

  object InstanceState {
    def apply[K, V](cacheState: CacheState[K, V],
                    watches: Watches[K, V]): InstanceState[K, V] =
      new InstanceState(cacheState, watches)

    private[redis] def forEvalModify[K, V, A](tuple: (InstanceState[K, V], A)): (Watches[K, V], (CacheState[K, V], A)) =
      tuple._1.forEvalModify(tuple._2)
  }

  /** TODO update scaladoc
   * A local in-memory cache implementation of the `RedisCache` trait that ultimately operates
   * using an `AtomicCell`, supporting custom expiration times and race condition behaviors.
   *
   * This class implements `CacheState[StateT[F, InstanceState[K, V], *], K, V]` and
   * is then converted to an implementation in `F[_]` in its companion object. This allows us
   * to sequence a series of `StateT[F, InstanceState[K, V], Unit]` operations together
   * in our [[LocalRedisTransactions]], allowing them to operate as a fused
   * `InstanceState[K, V] => F[(InstanceState[K, V], Unit)]` passed to
   * [[AtomicCell.evalModify]]. Without this indirection, any `set` operations executed inside
   * the `evalModify` cause deadlocks, preventing the successful implementation of [[updateExisting]].
   *
   * @param watches     tracks watches key/values during transactional operations
   * @param verboseMode when true, emit quite a bit of detail about operations to the console. otherwise, be quiet
   * @tparam F the effect type in which to operate
   * @tparam K the type of the key in the cache
   * @tparam V the type of the value in the cache
   */
  private class LocalRedisCore[
    F[_] : MonadCancelThrow : Clock : Console : Trace,
    K: KeyEncoder : TraceableValue,
    V: Encoder : TraceableValue
  ] private[LocalCache](verboseMode: Boolean) extends RedisCore[StateT[F, CacheState[K, V], *], K, V] {
    private type StF[a] = StateT[F, CacheState[K, V], a]

    override def get(key: K): StateT[F, CacheState[K, V], Option[V]] = Trace[StF].span("LocalCache.get") {
      Trace[StF].put("key" -> key) >>
        StateT { (cache: CacheState[K, V]) =>
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

    override def set(key: K, value: V, setArgs: SetArgs): StateT[F, CacheState[K, V], Boolean] =
      Trace[StF].span("LocalCache.set") {
        Trace[StF].put("key" -> key, "value" -> value, "setArgs" -> setArgs) >>
          StateT { (cache: CacheState[K, V]) =>
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

    override def del(key: K): StateT[F, CacheState[K, V], Long] = Trace[StF].span("LocalCache.del") {
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

  /** TODO update scaladoc
   * Creates and initializes a new instance of [[CacheState]] with provided configurations and dependencies.
   *
   * This initializes a [[LocalCache]] instance, which is a `CacheState[StateT[F, InstanceState[K, V], *], K, V]`.
   * The `StateT` effect allows us to logically call [[LocalCache.set]] inside [[LocalCache.updateExisting]], which would
   * normally cause a deadlock, since [[LocalCache.updateExisting]] obtains exclusive access to update the underlying
   * `AtomicCell[F, InstanceState[K, V]]`. When the `set` operation is a `StateT`, it can be given the current
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
    transactionalRetryPolicy: StateT[F, InstanceState[K, V], *] ~> StateT[F, InstanceState[K, V], *],
   )
   (implicit Mk: Ref.Make[StateT[F, InstanceState[K, V], *]]): Resource[F, LocalCachePool[F, K, V]] =
    AtomicCell[F].of(Map.empty[K, (V, Option[Instant])])
      .toResource
      .flatMap { (cacheStateCell: AtomicCell[F, CacheState[K, V]]) =>
        KeyPool.Builder[F, Unit, AtomicCell[F, SortedMap[K, V]]] { (_: Unit) =>
            AtomicCell[F].of(SortedMap.empty[K, V]).toResource
          }
          .build
          .map {
            _.map { (instanceWatchesCell: AtomicCell[F, Watches[K, V]]) =>
              type StF[a] = StateT[F, InstanceState[K, V], a]

              val core: RedisCore[StF, K, V] =
                new LocalRedisCore[F, K, V](verboseMode).mapK(new UnfocusCacheStateToInstanceState[F, K, V])

              val txInstance = LocalRedisTransactions[F, K, V](verboseMode)
              val tx: TransactionalRedisCommandsAlgebra[StF, K, V] =
                TransactionalRedisCommandsAlgebra(core, txInstance, transactionalRetryPolicy)

              val impl = new RedisCache.RedisCacheImpl(core, tx)
                  .mapK(new MaterializeStateTransitionsUsingAtomicCell(cacheStateCell, instanceWatchesCell))
                  .mapK(new ApplyCacheOperationModifications(explodeWhen, sleepAfterWhen))

              // TODO it feels like with more development effort, this could be done generically with something from cats-tagless
              new RedisCache[F, K, V] {
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
          .map { (redisCachePool: KeyPool[F, Unit, RedisCache[F, K, V]]) =>
            new LocalCachePool[F, K, V] {
              override def cacheState: F[CacheState[K, V]] = cacheStateCell.get
              override def pool: KeyPool[F, Unit, RedisCache[F, K, V]] = redisCachePool
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
  }
}

/**
 * A local in-memory implementation of the redis4cats interfaces [[HighLevelTx]] with [[Watcher]].
 *
 * This is implemented in `StateT[F, InstanceState[K, V], *]` so that the list of `F[Unit]`s
 * passed to [[transact]] and [[transact_]] can be run inside the exclusive modification of an
 * [[AtomicCell]] without causing a deadlock.
 *
 * @tparam K Type of the key used in the cache
 * @tparam V Type of the value stored in the cache
 * @param watched The currently watched keys and their values when each watch was started. Passed as a constructor
 *                parameter because initializing the [[Ref]] is effectful.
 * @param verboseMode when true, emit quite a bit of detail about operations to the console. otherwise, be quiet
 */
class LocalRedisTransactions[F[_] : MonadCancelThrow : Clock : Console : Trace, K: Ordering : KeyEncoder, V: Eq : Encoder](verboseMode: Boolean)
                                                                                                                          (implicit Mk: Ref.Make[StateT[F, InstanceState[K, V], *]])
  extends HighLevelTx[StateT[F, InstanceState[K, V], *]]
    with Watcher[StateT[F, InstanceState[K, V], *], K] {
  private type StF[a] = StateT[F, InstanceState[K, V], a]

  private def checkWatchesAndThenTransact[A](fa: StateT[F, InstanceState[K, V], A]): StateT[F, InstanceState[K, V], A] =
    Console[StF].println("entering LocalWatcher.checkWatchesAndThenTransact").whenA(verboseMode) >>
      Trace[StF].span("LocalWatcher.checkWatchesAndThenTransact") {
        StateT
          .inspectF[F, InstanceState[K, V], Unit] { instanceState =>
            TransactionDiscarded.raiseError.unlessA {
              instanceState.watches.forall { case (k, v) =>
                instanceState.cacheState.get(k).exists(_._1 === v)
              }
            }
          }
          .flatMap(_ => fa)
          .guarantee(unwatch)
          .guarantee(Console[StF].println("leaving LocalWatcher.checkWatchesAndThenTransact").whenA(verboseMode))
      }

  override def transact[A](fs: TxStore[StateT[F, InstanceState[K, V], *], String, A] => List[StateT[F, InstanceState[K, V], Unit]]): StateT[F, InstanceState[K, V], Map[String, A]] =
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

  override def transact_(fs: List[StateT[F, InstanceState[K, V], Unit]]): StateT[F, InstanceState[K, V], Unit] =
    checkWatchesAndThenTransact(Trace[StF].span("LocalWatcher.transact_")(fs.sequence_))

  override def watch(keys: K*): StateT[F, InstanceState[K, V], Unit] = Trace[StF].span("LocalWatcher.watch") {
    StateT.modifyF { instanceState =>
      for {
        now <- Clock[F].realTimeInstant
        newWatches =
          keys.filterNot(instanceState.watches.keySet) // don't update existing watches
            .foldLeft(Map.empty[K, V]) { (acc, key) =>
              getFromCacheWithTtl(instanceState.cacheState, now, key) match {
                case CacheGet.Hit(v, _) => acc.updated(key, v)
                case _ => acc
              }
            }

        watchState = instanceState.watches ++ newWatches
        _ <- Trace[F].put("updated_watch_state" -> watchState.asJson)
      } yield instanceState.withWatches(watchState)
    }
  }

  override def unwatch: StateT[F, InstanceState[K, V], Unit] =
    Trace[StF].span("LocalWatcher.unwatch") {
      Console[StF].println("entering LocalWatcher.unwatch").whenA(verboseMode) >>
        StateT.set[F, Watches[K, V]](SortedMap.empty)
          .transformS[InstanceState[K, V]](_.watches, _.withWatches(_))
          .guarantee(Console[StF].println("leaving LocalWatcher.unwatch").whenA(verboseMode))
    }
}

object LocalRedisTransactions {
  def apply[F[_] : MonadCancelThrow : Clock : Console : Trace, K: Ordering : KeyEncoder, V: Eq : Encoder](verboseMode: Boolean)
                                                                                                         (implicit Mk: Ref.Make[StateT[F, InstanceState[K, V], *]]): HighLevelTx[StateT[F, InstanceState[K, V], *]] & Watcher[StateT[F, InstanceState[K, V], *], K] =
    new LocalRedisTransactions(verboseMode)
}

object LocalCacheBuilder {
  def apply[F[_] : Temporal : Trace, K, V] = new LocalCacheBuilder[F, K, V](
    explodeWhen = PartialFunction.empty,
    sleepAfterWhen = PartialFunction.empty,
    verboseMode = false,
    transactionalRetryPolicy = new TransactionalRetryPolicy[StateT[F, InstanceState[K, V], *]]
  )
}

class LocalCacheBuilder[F[_], K, V] private(explodeWhen: PartialFunction[CacheOperation[K, V], Throwable],
                                            sleepAfterWhen: PartialFunction[CacheOperation[K, V], FiniteDuration],
                                            verboseMode: Boolean,
                                            transactionalRetryPolicy: StateT[F, InstanceState[K, V], *] ~> StateT[F, InstanceState[K, V], *]) {
  def explodingWhen(pf: PartialFunction[CacheOperation[K, V], Throwable]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(pf, sleepAfterWhen, verboseMode, transactionalRetryPolicy)

  def sleepingAfter(pf: PartialFunction[CacheOperation[K, V], FiniteDuration]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, pf, verboseMode, transactionalRetryPolicy)

  def verbose(): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode = true, transactionalRetryPolicy)

  def withVerbose(b: Boolean): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode = b, transactionalRetryPolicy)

  def withStatefulTransactionalRetryPolicy(trp: StateT[F, InstanceState[K, V], *] ~> StateT[F, InstanceState[K, V], *]): LocalCacheBuilder[F, K, V] =
    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode, trp)

  def withTransactionalRetryPolicy(fk: F ~> F)
                                  (implicit F: Monad[F]): LocalCacheBuilder[F, K, V] = {
    val trp = new (StateT[F, InstanceState[K, V], *] ~> StateT[F, InstanceState[K, V], *]) {
      override def apply[A](fa: StateT[F, InstanceState[K, V], A]): StateT[F, InstanceState[K, V], A] =
        fa.transformF(fk(_))
    }

    new LocalCacheBuilder(explodeWhen, sleepAfterWhen, verboseMode, trp)
  }


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
            Mk: Ref.Make[StateT[F, InstanceState[K, V], *]]): Resource[F, LocalCachePool[F, K, V]] =
    LocalCache(explodeWhen, sleepAfterWhen, verboseMode, transactionalRetryPolicy)
}
