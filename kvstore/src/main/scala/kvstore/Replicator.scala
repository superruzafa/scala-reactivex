package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class ReplicateStatus(
    id: Long,
    key: String,
    valueOption: Option[String],
    primary: ActorRef,
    timeout: Cancellable
  )

  case class SnapshotTimeout(seq: Long)

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
  var replicates = Map.empty[Long, ReplicateStatus]
  
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
      val rs = ReplicateStatus(
        id = id,
        key = key,
        valueOption = valueOption,
        primary = sender,
        timeout =
          context.system.scheduler.schedule(
            100.milliseconds, 100.milliseconds, self, SnapshotTimeout(seq))
      )
      replicates += ((seq, rs))
      replica ! Snapshot(key, valueOption, seq)

    case SnapshotTimeout(seq) =>
      replicates.get(seq) match {
        case Some(rs) => replica ! Snapshot(rs.key, rs.valueOption, seq)
        case None =>
      }

    case SnapshotAck(key, seq) =>
      replicates.get(seq) match {
        case Some(rs) =>
          replicates -= seq
          rs.timeout.cancel
          rs.primary ! Replicated(key, rs.id)
        case None =>
      }
  }

}
