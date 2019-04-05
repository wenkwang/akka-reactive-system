package protocols

import akka.actor.typed.{ActorContext, _}
import akka.actor.typed.scaladsl._

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
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = new ExtensibleBehavior[T]() {

      private val buffer = StashBuffer[T](capacity = bufferSize)
      private val bufferLimit = bufferSize

      private def nextBehaviour(ctx: ActorContext[T], next: Behavior[T]): Behavior[T] = {
        val nextCanonical = Behavior.canonicalize(next, this, ctx)
        if (nextCanonical != this) {
          if (!Behavior.isAlive(nextCanonical)) nextCanonical
          else buffer.unstashAll(ctx.asScala, SelectiveReceive(bufferSize, nextCanonical))
        }
        else nextCanonical
      }

      override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
        import akka.actor.typed.Behavior.{ start, validateAsInitial, interpretMessage }
//        println(s"Receive message: ${msg.toString}")
//        print(s"Current buffer:")
        buffer.foreach(msg => print(msg + ", "))
        println()
        val started = validateAsInitial(start(initialBehavior, ctx))
        val next = interpretMessage(started, ctx, msg)
        if (Behavior.isUnhandled(next)) {
//          println(s"Unhandled...")
          buffer.stash(msg)
          if (buffer.size > bufferLimit)
          // Stopping this actor with error, can be handled by supervisor
            throw new StashOverflowException("Buffer overflow")
          // Returning unhandled behaviour
          next
        } else {
//          println(s"Handled...")
          nextBehaviour(ctx, next)
        }
      }

      override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = Behavior.interpretSignal(initialBehavior, ctx, msg)
    }
}
