package kvstore

import akka.actor.{ActorRef, Actor, Cancellable, OneForOneStrategy, Props}
import kvstore.Arbiter._
import scala.concurrent.duration._

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
    id: Long,
    key: String,
    valueOption: Option[String],
    client: ActorRef,
    remainingReplicators: Set[ActorRef],
    persisted: Boolean,
    operationTimeout: Cancellable,
    persistTimeout: Cancellable
  )

  def isOperationDone(op: OperationStatus): Boolean =
    op.persisted && op.remainingReplicators.isEmpty

  def finishOperation(op: OperationStatus, ack: Boolean): Unit = {
    op.operationTimeout.cancel
    val message = if (ack) OperationAck(op.id) else OperationFailed(op.id)
    op.client ! message
  }

  case class SnapshotStatus(
    seq: Long,
    key: String,
    valueOption: Option[String],
    replicator: ActorRef,
    persistTimeout: Cancellable
  )

  def finishSnapshot(s: SnapshotStatus): Unit = {
    s.persistTimeout.cancel
    s.replicator ! Replicator.SnapshotAck(s.key, s.seq)
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
  var secondaries = Map.empty[ActorRef, ActorRef]
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

  // Common behavior for both leaders and secondary replicas
  val common: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }

  def runUpdate(key: String, valueOption: Option[String], id: Long): Unit = {
      val op = OperationStatus(
        id = id,
        key = key,
        valueOption = valueOption,
        client = sender,
        remainingReplicators = replicators,
        persisted = false,
        persistTimeout =
          context.system.scheduler.schedule(
            100.milliseconds, 100.milliseconds, self, PersistTimeout(id)),
        operationTimeout =
          context.system.scheduler.scheduleOnce(
            1.second, self, OperationTimeout(id))
      )
      operations += ((id, op))
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
        case Some(op) => persistence ! Persist(op.key, op.valueOption, id)
        case None =>
      }

    case Persisted(_, id) =>
      operations.get(id) match {
        case Some(op) =>
          val newOp = op.copy(persisted = true)
          if (isOperationDone(newOp)) {
            operations -= id
            finishOperation(newOp, true)
          } else {
            operations += ((id, newOp))
            newOp.persistTimeout.cancel
          }
        case None =>
      }
      ()

    case Replicated(key, id) =>
      operations.get(id) match {
        case Some(op) =>
          val newOp = op.copy(
            remainingReplicators = op.remainingReplicators - sender)
          if (isOperationDone(newOp)) {
            operations -= id
            finishOperation(newOp, true)
          } else {
            operations += ((id, newOp))
          }
        case None =>
      }

    case OperationTimeout(id) =>
      operations.get(id) match {
        case Some(op) =>
          operations -= id
          finishOperation(op, false)
        case None =>
      }

    case Replicas(replicas) =>
      val nextReplicas = replicas - self
      val currentReplicas = secondaries.keySet

      val newReplicas = nextReplicas -- currentReplicas
      val droppedReplicas = currentReplicas -- nextReplicas
      val newSecondaries = newReplicas.map { replica =>
        (replica, context.actorOf(Replicator.props(replica)))
      }
      val droppedSecondaries = secondaries.filterKeys { replica =>
        droppedReplicas.contains(replica)
      }
      val newReplicators = newSecondaries.map { _._2 }
      val droppedReplicators = droppedReplicas.map { secondaries(_) }

      secondaries = secondaries -- droppedSecondaries.keySet ++ newSecondaries
      replicators = replicators -- droppedReplicators ++ newReplicators

      operations.map { case (id, op) =>
        val newOp = op.copy(remainingReplicators = op.remainingReplicators -- droppedReplicators)
        if (isOperationDone(newOp)) {
          operations -= id
          finishOperation(newOp, true)
        } else {
          operations += ((id, newOp))
        }
      }

      for {
        replicator <- newReplicators
        (key, value) <- kv
      }
        replicator ! Replicate(key, Some(value), 0L)

      droppedReplicators.foreach { context.stop(_) }
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
        val snapshot = SnapshotStatus(
          seq = seq,
          key = key,
          valueOption = valueOption,
          replicator = sender,
          persistTimeout =
            context.system.scheduler.schedule(
              100.milliseconds, 100.milliseconds, self, PersistTimeout(seq))
        )
        snapshots += ((seq, snapshot))
        persistence ! Persist(key, valueOption, seq)
      }

    case PersistTimeout(seq) =>
      snapshots.get(seq) match {
        case Some(s) => persistence ! Persist(s.key, s.valueOption, seq)
        case None =>
      }

    case Persisted(key, seq) =>
      snapshots.get(seq) match {
        case Some(s) =>
          snapshots -= seq
          finishSnapshot(s)
          expectedSeq = math.max(expectedSeq, seq + 1L)
        case None =>
      }
  }

  arbiter ! Arbiter.Join
}

