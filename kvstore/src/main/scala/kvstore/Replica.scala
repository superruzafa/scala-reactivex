package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
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
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  private case class TryPersist(key: String, valueOption: Option[String], id: Long) {
    val persistMsg = Persist(key, valueOption, id)
  }

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
  var senderById = Map.empty[Long, ActorRef]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // Common behavior for both leaders and secondary replicas
  val common: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = common.orElse {
    case Insert(key, value, id) =>
      kv = kv + ((key, value))
      sender ! OperationAck(id)
    case Remove(key, id) =>
      kv = kv - key
      sender ! OperationAck(id)
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
        senderById = senderById + ((seq, sender))
        self ! TryPersist(key, valueOption, seq)
      }

    case msg @ TryPersist(_, _, seq) =>
      if (seq == expectedSeq) {
        context.system.scheduler.scheduleOnce(100.milliseconds, self, msg)
        persistence ! msg.persistMsg
      }

    case Persisted(key, seq) =>
      senderById.get(seq).foreach { _ ! SnapshotAck(key, seq) }
      senderById = senderById - seq
      expectedSeq = math.max(expectedSeq, seq + 1L)
  }

  arbiter ! Arbiter.Join
}

