package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor, Cancellable }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  case class OperationStatus(
    key: String,
    valueOption: Option[String],
    senderActor: ActorRef,
    remainingReplicators: Set[ActorRef],
    persisted: Boolean,
    operationTimeout: Cancellable,
    persistTimeout: Cancellable
  ) {
    def done(): Unit = {
      operationTimeout.cancel
      persistTimeout.cancel
      ()
    }
  }

  case class SnapshotStatus(
    key: String,
    valueOption: Option[String],
    senderActor: ActorRef,
    persistTimeout: Cancellable
  ) {
    def done(): Unit = {
      persistTimeout.cancel
      ()
    }
  }

  case class PersistTimeout(id: Long)
  case class OperationTimeout(id: Long)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var replicatorBySecondary = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistence = context.actorOf(persistenceProps)
  var expectedSeq = 0L
  var operations = Map.empty[Long, OperationStatus]
  var snapshots = Map.empty[Long, SnapshotStatus]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def isOperationDone(status: OperationStatus): Boolean = {
    status.remainingReplicators.isEmpty && status.persisted
  }

  // Common behavior for both leaders and secondary replicas
  val common: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
  }

  def runUpdate(key: String, valueOption: Option[String], id: Long): Unit = {
      val persistTimeout = context.system.scheduler.schedule(
        100.milliseconds, 100.milliseconds, self, PersistTimeout(id))
      val operationTimeout = context.system.scheduler.scheduleOnce(
        1.second, self, OperationTimeout(id))
      val status = OperationStatus(
        key = key,
        valueOption = valueOption,
        senderActor = sender,
        remainingReplicators = replicators,
        persisted = false,
        persistTimeout = persistTimeout,
        operationTimeout = operationTimeout
      )
      operations += ((id, status))
      persistence ! Persist(key, valueOption, id)
      replicators.foreach { _ ! Replicate(key, valueOption, id) }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = common.orElse {
    case Insert(key, value, id) =>
      kv = kv + ((key, value))
      runUpdate(key, Some(value), id)

    case Remove(key, id) =>
      kv = kv - key
      runUpdate(key, None, id)

    case PersistTimeout(id) =>
      operations.get(id) match {
        case Some(status) => persistence ! Persist(status.key, status.valueOption, id)
        case None =>
      }

    case Persisted(_, id) =>
      operations.get(id) match {
        case Some(status) =>
          val newStatus = status.copy(persisted = true)
          if (isOperationDone(newStatus)) {
            status.done
            status.senderActor ! OperationAck(id)
            operations -= id
          } else {
            operations += ((id, newStatus))
            newStatus.persistTimeout.cancel
          }
        case None =>
      }
      ()

    case Replicated(key, id) =>
      operations.get(id) match {
        case Some(status) =>
          val newStatus = status.copy(
            remainingReplicators = status.remainingReplicators - sender)
          if (isOperationDone(newStatus)) {
            status.done
            status.senderActor ! OperationAck(id)
            operations -= id
          } else {
            operations += ((id, newStatus))
          }
        case None =>
      }

    case OperationTimeout(id) =>
      operations.get(id) match {
        case Some(status) =>
          operations -= id
          status.done
          status.senderActor ! OperationFailed(id)
        case None =>
      }

    case Replicas(replicas) =>
      val nextSecondaries = replicas - self
      val currentSecondaries = replicatorBySecondary.keySet

      val newSecondaries = nextSecondaries -- currentSecondaries
      val oldSecondaries = currentSecondaries -- nextSecondaries

      val newSecondRep = newSecondaries.map { s =>
        (s, context.actorOf(Replicator.props(s)))
      }
      val oldSecondRep = oldSecondaries.map { s =>
        (s, replicatorBySecondary(s))
      }

      val newReplicators = newSecondRep.toMap.values
      val oldReplicators = oldSecondRep.toMap.values

      for {
        r <- newReplicators
        (k, v) <- kv
      }
        r ! Replicate(k, Some(v), 0L)
      oldReplicators.foreach { context.stop(_) }

      replicators = replicators -- oldReplicators ++ newReplicators
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = common.orElse {
    case Snapshot(key, valueOption, seq) =>
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq) {
        valueOption match {
          case Some(value) => kv = kv + ((key, value))
          case None => kv = kv - key
        }
        val persistTimeout = context.system.scheduler.schedule(
          100.milliseconds, 100.milliseconds, self, PersistTimeout(seq))
        val status = SnapshotStatus(
          key = key,
          valueOption = valueOption,
          senderActor = sender,
          persistTimeout = persistTimeout
        )
        snapshots += ((seq, status))
        persistence ! Persist(key, valueOption, seq)
      }

    case PersistTimeout(seq) =>
      snapshots.get(seq) match {
        case Some(status) => persistence ! Persist(status.key, status.valueOption, seq)
        case None =>
      }

    case Persisted(key, seq) =>
      snapshots.get(seq) match {
        case Some(status) =>
          snapshots = snapshots - seq
          status.senderActor ! SnapshotAck(key, seq)
          status.done
          expectedSeq = math.max(expectedSeq, seq + 1L)
        case None =>
      }
  }

  arbiter ! Arbiter.Join
}

