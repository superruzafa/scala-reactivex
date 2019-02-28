package kvstore

import akka.actor.{ActorRef, Actor, OneForOneStrategy, SupervisorStrategy}

object Arbiter {
  case object Join

  case object JoinedPrimary
  case object JoinedSecondary

  /**
   * This message contains all replicas currently known to the arbiter, including the primary.
   */
  case class Replicas(replicas: Set[ActorRef])
}

class Arbiter extends Actor {
  import Arbiter._
  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception =>
      replicas -= sender
      leader.foreach { _ ! Replicas(replicas) }
      SupervisorStrategy.Restart
  }

  def receive = {
    case Join =>
      if (leader.isEmpty) {
        leader = Some(sender)
        sender ! JoinedPrimary
      } else {
        sender ! JoinedSecondary
      }
      replicas += sender
      leader foreach { _ ! Replicas(replicas) }
  }

}
