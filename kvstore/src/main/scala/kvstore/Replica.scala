package kvstore

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, ReceiveTimeout, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import akka.japi

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  var acks = Map.empty[Long, (ActorRef, Persist)]

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => SupervisorStrategy.restart
  }

  var persistence = context.actorOf(persistenceProps)
  context.watch(persistence)

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = insert(kv, key, value)
      acks = insert(acks, id, (sender, Persist(key, Some(value), id)))
      persistence ! Persist(key, Some(value), id)
      context.setReceiveTimeout(1.second)
    }
    case Remove(key, id) => {
      kv = remove(kv, key)
      acks = insert(acks, id, (sender, Persist(key, None, id)))
      persistence ! Persist(key, None, id)
      context.setReceiveTimeout(1.second)
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Replicas(replicas) => {
      val (replicasToAdd, replicasToRemove) = parseOutTargetReplicas(replicas)
      addReplicas(replicasToAdd)
      removeReplicas(replicasToRemove)
    }
    case Replicated =>
    case Persisted(key, id) =>
//      println(s"Persisted: key-$key, id-$id")
      val entry = acks(id)
      acks = remove(acks, id)
      if (acks.isEmpty) context.setReceiveTimeout(Duration.Undefined)
      else context.setReceiveTimeout(1.second)
      entry._1 ! OperationAck(id)
      context.system.eventStream.publish(Replicate(entry._2.key, entry._2.valueOption, entry._2.id))
    case ReceiveTimeout =>
      acks.foreach(entry => persistence ! entry._2._2)
      context.setReceiveTimeout(1.second)

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => {
      val result = kv.get(key)
      sender ! GetResult(key, result, id)
    }
    case snapshot: Snapshot if snapshot.seq == _seqCounter => {
//      println(s"== seq: $seq, seqCounter: ${_seqCounter}, value: $valueOption")
      localPersist(snapshot.key, snapshot.valueOption)
      nextSeq()
      val persist = Persist(snapshot.key, snapshot.valueOption, snapshot.seq)
      acks = insert(acks, snapshot.seq, (sender, persist))
      persistence ! persist
      context.setReceiveTimeout(100.milliseconds)
    }
    case snapshot: Snapshot if snapshot.seq < _seqCounter => {
//      println(s"< seq: $seq, seqCounter: ${_seqCounter}")
      sender ! SnapshotAck(snapshot.key, snapshot.seq)
    }
    case Persisted(key, seq) =>
      val entry = acks(seq)
      acks = remove(acks, seq)
      if (acks.isEmpty) context.setReceiveTimeout(Duration.Undefined)
      else context.setReceiveTimeout(100.milliseconds)
      entry._1 ! SnapshotAck(key, seq)
    case ReceiveTimeout =>
      acks.foreach(entry => persistence ! entry._2._2)
      context.setReceiveTimeout(100.milliseconds)
  }

  private def insert[A, B](map: Map[A, B], key: A, value: B): Map[A, B]  = {
//    println(s"insert ($key, $value)")
//    println(kv.mkString("|"))
    map + (key -> value)
  }

  private def remove[A, B](map: Map[A, B], key: A): Map[A, B] = {
//    println(s"remove $key")
//    println(kv.mkString("|"))
    if (map.contains(key)) map - key
    else map
  }

  private def localPersist(key: String, valueOption: Option[String]) = {
    valueOption match {
      case Some(x) => kv = insert(kv, key, x)
      case None => kv = remove(kv, key)
    }
  }

  private def parseOutTargetReplicas(replicas: Set[ActorRef]): (Set[ActorRef], Set[ActorRef]) = {
    (replicas.diff(secondaries.keySet), secondaries.keySet.diff(replicas))
  }

  private def addReplicas(replicasToAdd: Set[ActorRef]) = {
    if (replicasToAdd.nonEmpty) {
      secondaries = replicasToAdd.foldLeft(secondaries)((map, replica) => {
        val replicator = context.actorOf(Replicator.props(replica))
        forwardUpdateEvents(replicator)
        replicators += replicator
        map + (replica -> replicator)
      })
    }
  }

  private def forwardUpdateEvents(replicator: ActorRef) = {
    kv.foreach(entry => replicator.forward(Replicate(entry._1, Some(entry._2), -1)))
  }

  private def removeReplicas(replicasToRemove: Set[ActorRef]) = {
    if (replicasToRemove.nonEmpty) {
      secondaries = replicasToRemove.foldLeft(secondaries)((map, replica) => {
        map.get(replica) match {
          case Some(x) => {
            context.stop(x)
            replicators -= x
            map - replica
          }
          case None => map
        }
      })
    }
  }
}

