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

  var replicateACKS = Map.empty[Long, (ActorRef, Int)]

  var replicatedACKS = Map.empty[Long, Set[ActorRef]]

  var _seqCounter = 0L
  def nextSeq(): Long = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  var persistACKs = Map.empty[Long, (ActorRef, Persist)]

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => SupervisorStrategy.restart
  }

  var persistence = context.actorOf(persistenceProps)
  context.watch(persistence)

  arbiter ! Join

  context.system.scheduler.schedule(0.milliseconds, 100.milliseconds)(sendPersists)

  def receive = {
    case JoinedPrimary   => {
      context.system.eventStream.subscribe(self, classOf[Replicated])
      context.become(leader)
    }
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
//      println(s"Receive Insert: key-$key, value-$value, id-$id")
      kv = insert(kv, key, value)
      persistACKs = insert(persistACKs, id, (sender, Persist(key, Some(value), id)))
      persistence ! Persist(key, Some(value), id)
      context.setReceiveTimeout(1.second)
    }
    case Remove(key, id) => {
//      println(s"Receive Remove: key-$key, id-$id")
      kv = remove(kv, key)
      persistACKs = insert(persistACKs, id, (sender, Persist(key, None, id)))
      persistence ! Persist(key, None, id)
      context.setReceiveTimeout(1.second)
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Replicas(replicas) => {
//      println(s"Receive Replicas: ${replicas.mkString("|")}, size-${secondaries.size}, secondaries: ${secondaries.mkString("|")}, replicas: ${replicas.mkString("|")}")
      val (replicasToAdd, replicasToRemove) = parseOutTargetReplicas(replicas)
      addReplicas(replicasToAdd)
      removeReplicas(replicasToRemove)
//      println(s"Secondaries: size-${secondaries.size}")
    }
    case Persisted(key, id) =>
//      println(s"Receive Persisted: key-$key, id-$id")
      val entry = persistACKs(id)
      persistACKs = remove(persistACKs, id)
//      println(s"Secondaries: size-${secondaries.size}")
      if (secondaries.isEmpty) {
        entry._1 ! OperationAck(id)
        context.setReceiveTimeout(Duration.Undefined)
      } else {
        val replicate = Replicate(entry._2.key, entry._2.valueOption, entry._2.id)
        replicateACKS = insert(replicateACKS, id, (entry._1, 0))
        replicatedACKS = insert(replicatedACKS, id, Set.empty[ActorRef])
        context.system.eventStream.publish(replicate)
      }
    case Replicated(key, id) => {
      //      println(s"Receive Replicated: key-$key, id-$id")
      replicateACKS.get(id) match {
        case Some(entry) => {
          val count = entry._2 + 1
          val client = entry._1
          if (count >= secondaries.size) {
            client! OperationAck(id)
            replicateACKS = remove(replicateACKS, id)
            replicatedACKS = remove(replicatedACKS, id)
            context.setReceiveTimeout(Duration.Undefined)
          } else {
            replicateACKS = insert(replicateACKS, id, (client, count))
            val set = replicatedACKS(id) + sender
            replicatedACKS = insert(replicatedACKS, id, set)
          }
        }
        case None =>
      }
    }
    case ReceiveTimeout =>
      //      println("ReceiveTimeout...")
      //      println("acks: " + persistACKs.mkString("|"))
      //      println("replicates: " + replicateACKS.mkString("|"))
      //      println("secondaries: " + secondaries.mkString("|"))
      persistACKs.foreach(entry => entry._2._1 ! OperationFailed(entry._1))
      replicateACKS.foreach(entry => entry._2._1 ! OperationFailed(entry._1))
      persistACKs = Map.empty[Long, (ActorRef, Persist)]
      replicateACKS = Map.empty[Long, (ActorRef, Int)]
      replicatedACKS = Map.empty[Long, Set[ActorRef]]
      context.setReceiveTimeout(Duration.Undefined)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => {
      val result = kv.get(key)
      sender ! GetResult(key, result, id)
    }
    case snapshot: Snapshot if snapshot.seq == _seqCounter => {
//      println(s"[Replica] Receive Snapshot(=): key-${snapshot.key}, value-${snapshot.valueOption}")
      localPersist(snapshot.key, snapshot.valueOption)
      nextSeq()
      val persist = Persist(snapshot.key, snapshot.valueOption, snapshot.seq)
      persistACKs = insert(persistACKs, snapshot.seq, (sender, persist))
      persistence ! persist
//      context.setReceiveTimeout(100.milliseconds)
    }
    case snapshot: Snapshot if snapshot.seq < _seqCounter => {
//      println(s"[Replica] Receive Snapshot (<): key-${snapshot.key}, value-${snapshot.valueOption}")
      sender ! SnapshotAck(snapshot.key, snapshot.seq)
    }
    case Persisted(key, seq) =>
//      println(s"[Replica] Receive Persisted: key-$key, value-$seq")
      val entry = persistACKs(seq)
      persistACKs = remove(persistACKs, seq)
//      if (acks.isEmpty) context.setReceiveTimeout(Duration.Undefined)
//      else context.setReceiveTimeout(1.second)
      entry._1 ! SnapshotAck(key, seq)
    case _ =>
//      println(s"[Replica] Receive unexpected message." )
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
    (replicas.filterNot(_ == self).diff(secondaries.keySet), secondaries.keySet.diff(replicas))
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
    var seq = 0L
    kv.foreach(entry => {
      replicator.forward(Replicate(entry._1, Some(entry._2), seq))
      seq += 1
    })
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
      replicatedACKS.foreach { entry =>
        val id = entry._1
        if (entry._2 == replicators) {
          replicateACKS(id)._1 ! OperationAck(id)
          replicateACKS = remove(replicateACKS, id)
          replicatedACKS = remove(replicatedACKS, id)
        }
      }
    }
  }

  private def sendPersists() = {
    persistACKs.foreach(entry => persistence ! entry._2._2)
  }

}

