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

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  context.system.eventStream.subscribe(self, classOf[Replicate])

  context.system.scheduler.schedule(0.milliseconds, 100.milliseconds)(sendSnapshots(replica))

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
    case replicate: Replicate => {
      val seq = nextSeq
      acks = insert(seq, (sender, replicate))
      replica ! Snapshot(replicate.key, replicate.valueOption, seq)
    }
    case snapshotAck: SnapshotAck => {
      acks.get(snapshotAck.seq).foreach(entry => entry._1 ! Replicated(entry._2.key, entry._2.id))
      acks = remove(snapshotAck.seq)
    }
    case _ =>
  }

  private def insert(key: Long, value: (ActorRef, Replicate)): Map[Long, (ActorRef, Replicate)] = {
    acks + (key -> value)
  }

  private def remove(key: Long): Map[Long, (ActorRef, Replicate)] = {
    acks - key
  }

  private def sendSnapshots(replica: ActorRef) = {
    acks.foreach(entry => replica ! Snapshot(entry._2._2.key, entry._2._2.valueOption, entry._1))
  }

  override def postStop(): Unit = context.system.eventStream.unsubscribe(self)

}
