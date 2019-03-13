/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case request: Operation => context.become(runNext(Queue(request)))
    case GC => {
      val newRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
      root ! CopyTo(newRoot)
      pendingQueue = Queue.empty[Operation]
      context.become(garbageCollecting(newRoot))
    }
  }

  def running(queue: Queue[Operation]): Receive = {
    case reply: OperationReply => {
      queue.head.requester ! reply
      context.become(runNext(queue.tail))
    }
    case request: Operation => context.become(enqueue(queue, request))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case request: Operation => pendingQueue.enqueue(request)
    case CopyFinished => {
      context.stop(root)
      root = newRoot
      context.become(runNext(pendingQueue))
    }
    case GC =>
  }


  private def runNext(queue: Queue[Operation]): Receive = {
    if (queue.isEmpty) normal
    else {
//      println(s"Yao Test - ${queue.head.toString}")
      root ! queue.head
      running(queue)
    }
  }

  private def enqueue(queue: Queue[Operation], operation: Operation): Receive = {
    running(queue :+ operation)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case request: Operation => handleOperation(request)
    case reply: OperationReply => context.parent ! reply
    case copyTo: CopyTo => handleCopyTo(copyTo)
  }

  private def handleOperation(operation: Operation) = {
    operation match {
      case insertReq: Insert => {
        insertNode(insertReq)
      }
      case containsReq: Contains => {
        containNode(containsReq)
      }
      case removeReq: Remove => {
        removeNode(removeReq)
      }
    }
  }

  private def insertNode(insertReq: Insert) = {
    if (insertReq.elem == elem && removed) {
      removed = false
      context.parent ! OperationFinished(insertReq.id)
    } else if (insertReq.elem < elem) {
      insertHelper(Left, insertReq)
    } else if (insertReq.elem > elem) {
      insertHelper(Right, insertReq)
    } else {
      context.parent ! OperationFinished(insertReq.id)
    }
  }

  private def insertHelper(pos: Position, insertReq: Insert) = {
    subtrees.get(pos) match {
      case Some(x) => x ! insertReq
      case None => {
        val node = context.actorOf(BinaryTreeNode.props(insertReq.elem, false))
        subtrees += (pos -> node)
        context.parent ! OperationFinished(insertReq.id)
      }
    }
  }

  private def containNode(containsReq: Contains) = {
    if (containsReq.elem == elem) {
      context.parent ! ContainsResult(containsReq.id, !removed)
    } else if (containsReq.elem < elem) {
      containHelper(Left, containsReq)
    } else {
      containHelper(Right, containsReq)
    }
  }

  private def containHelper(pos: Position, containsReq: Contains) = {
    subtrees.get(pos) match {
      case Some(x) => x ! containsReq
      case None => context.parent ! ContainsResult(containsReq.id, false)
    }
  }

  private def removeNode(removeReq: Remove) = {
    if (removeReq.elem == elem) {
      removed = true
      context.parent ! OperationFinished(removeReq.id)
    } else if (removeReq.elem < elem) {
      removeHelper(Left, removeReq)
    } else {
      removeHelper(Right, removeReq)
    }
  }

  private def removeHelper(pos:Position, removeReq: Remove) = {
    subtrees.get(pos) match {
      case Some(x) => x ! removeReq
      case None => context.parent ! OperationFinished(removeReq.id)
    }
  }

  private def handleCopyTo(copyTo: CopyTo) = {
    if (!removed) copyTo.treeNode ! Insert(self, elem, elem)
    subtrees.values.foreach(_ ! copyTo)
    if (subtrees.isEmpty && removed) {
      context.parent ! CopyFinished
      context.stop(self)
    }
    else context.become(copying(subtrees.values.toSet, removed))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      }
      else context.become(copying(expected, true))
    }
    case CopyFinished => {
      val newExpected = expected - context.sender()
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
      }
      else context.become(copying(newExpected, insertConfirmed))
    }
  }

}
