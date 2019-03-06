package protocols

import akka.actor.typed._
import akka.actor.typed.Behavior.{ canonicalize, interpretMessage, isUnhandled }
import scala.collection.immutable.Queue

class SelectiveReceiveBehavior[T](behavior: Behavior[T], pending: Queue[T])
  extends ExtensibleBehavior[T] {

  def retryPending(
    behavior: Behavior[T],
    pending: Queue[T],
    ctx: ActorContext[T]): (Behavior[T], Queue[T]) = {

      def _retryPending(
        behavior: Behavior[T],
        currPending: Queue[T],
        newPending: Queue[T]): (Behavior[T], Queue[T]) =
      {
        currPending.dequeueOption match {
          case None => (behavior, newPending)
          case Some((msg, tail)) =>
            val nextBehavior = interpretMessage(behavior, ctx, msg)
            if (isUnhandled(nextBehavior)) {
              _retryPending(behavior, tail, newPending.enqueue(msg))
            } else {
              val canonicalNext = canonicalize(nextBehavior, behavior, ctx)
              _retryPending(canonicalNext, newPending ++ tail, Queue.empty[T])
            }
        }
      }
      _retryPending(behavior, pending, Queue.empty[T])
  }

  def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
    val nextBehavior = interpretMessage(behavior, ctx, msg)
    if (isUnhandled(nextBehavior)) {
      new SelectiveReceiveBehavior(behavior, pending.enqueue(msg))
    } else {
      val canonicalNext = canonicalize(nextBehavior, behavior, ctx)
      val (newBehavior, newPending) = retryPending(canonicalNext, pending, ctx)
      new SelectiveReceiveBehavior(newBehavior, newPending)
    }
  }
  def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = ???
}

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] =
      new SelectiveReceiveBehavior(initialBehavior, Queue.empty[T])
}
