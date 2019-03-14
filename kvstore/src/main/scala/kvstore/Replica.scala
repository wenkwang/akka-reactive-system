package kvstore

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
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
    _seqCounter += 1
  }

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = insert(key, value)
      sender ! OperationAck(id)
      context.system.eventStream.publish(Replicate(key, Some(value), id))
    }
    case Remove(key, id) => {
      kv = remove(key)
      sender ! OperationAck(id)
      context.system.eventStream.publish(Replicate(key, None, id))
    }
    case Get(key, id) => {
      sender ! GetResult(key, get(key), id)
    }
    case Replicas(replicas) => {
      val (replicasToAdd, replicasToRemove) = parseOutTargetReplicas(replicas)
      addReplicas(replicasToAdd)
      removeReplicas(replicasToRemove)
    }
    case Replicated =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => {
      val result = get(key)
      sender ! GetResult(key, result, id)
    }
    case Snapshot(key, valueOption, seq) if seq == _seqCounter => {
//      println(s"== seq: $seq, seqCounter: ${_seqCounter}, value: $valueOption")
      persist(key, valueOption)
      nextSeq()
      sender ! SnapshotAck(key, seq)
    }
    case Snapshot(key, _, seq) if seq < _seqCounter => {
//      println(s"< seq: $seq, seqCounter: ${_seqCounter}")
      sender ! SnapshotAck(key, seq)
    }
  }

  private def insert(key: String, value: String): Map[String, String]  = {
//    println(s"insert ($key, $value)")
//    println(kv.mkString("|"))
    kv + (key -> value)
  }

  private def remove(key: String): Map[String, String] = {
//    println(s"remove $key")
//    println(kv.mkString("|"))
    kv - key
  }

  private def get(key: String): Option[String] = {
//    println(s"get $key")
//    println(kv.mkString("|"))
    kv.get(key)
  }

  private def persist(key: String, valueOption: Option[String]) = {
    valueOption match {
      case Some(x) => kv = insert(key, x)
      case None => kv = remove(key)
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

