package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class Retry(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case msg @ Replicate(key, valueOption, id) =>
      val seq = nextSeq
      acks = acks + ((seq, (sender, msg)))
      self ! Retry(seq)

    case msg @ Retry(seq) =>
      acks.get(seq) match {
        case Some((_, Replicate(key, valueOption, id))) =>
          context.system.scheduler.scheduleOnce(100.milliseconds, self, msg)
          replica ! Snapshot(key, valueOption, seq)
        case None =>
      }

    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case Some((actor, Replicate(key, _, id))) =>
          acks = acks - seq
          actor ! Replicated(key, id)
        case None =>
      }
  }

}
