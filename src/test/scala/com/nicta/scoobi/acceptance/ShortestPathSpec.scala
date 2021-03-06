package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import ShortestPath._

class ShortestPathSpec extends NictaSimpleJobs {

  "The shortest path in a graph can be computed using Hadoop" >> { implicit sc: SC =>
    val paths =
      fromInput("A B", "A C", "C D", "C E", "D E", "F G", "E F", "G E").run { nodes: DList[String] =>

      val edges = nodes.map { n => val a :: b :: _ = n.split(" ").toList; (Node(a), Node(b)) }
      val adjacencies = edges.flatMap { case (first, second) => List((first, second), (second, first)) }
      val grouped = adjacencies.groupByKey[Node, Node]
      val startingPoint = Node("A")

      val formatted = grouped.map {
        case (node, es) if (node == startingPoint) => (node, NodeInfo(es, Frontier(0)))
        case (node, es)                            => (node, NodeInfo(es, Unprocessed()))
      }

      val iterations = 5
      val breadthResult = breadthFirst(formatted, iterations)

      breadthResult map {
        case (n: Node, ni: NodeInfo) => pathSize(ni.state) match {
          case None    => "Couldn't get to " + n.data + " in " + iterations + " steps"
          case Some(v) => "Shortest path from " + startingPoint.data + " to " + n.data + " is " + v + " steps"
        }
      }
    }
    paths must_== Seq("Shortest path from A to A is 0 steps",
                      "Shortest path from A to B is 1 steps",
                      "Shortest path from A to C is 1 steps",
                      "Shortest path from A to D is 2 steps",
                      "Shortest path from A to E is 2 steps",
                      "Shortest path from A to F is 3 steps",
                      "Shortest path from A to G is 3 steps")
  }
}


object ShortestPath {

  implicit val NodeOrderImp = NodeComparator

  implicit val unprocessedFormat            = mkCaseWireFormat(Unprocessed, Unprocessed.unapply _)
  implicit val frontierFormat               = mkCaseWireFormat(Frontier, Frontier.unapply _)
  implicit val doneFormat                   = mkCaseWireFormat(Done, Done.unapply _)
  implicit val progressFormat               = mkAbstractWireFormat[Progress, Unprocessed, Frontier, Done]()

  implicit val nodeFormat                   = mkCaseWireFormat(Node, Node.unapply _)
  implicit val nodeInfoFormat               = mkCaseWireFormat(NodeInfo, NodeInfo.unapply _)
  implicit val nodeGrouping: Grouping[Node] = OrderingGrouping[Node]

  case class Node(data: String)

  sealed trait Progress
  case class Unprocessed()       extends Progress
  case class Frontier(best: Int) extends Progress
  case class Done(best: Int)     extends Progress

  case class NodeInfo (edges: Iterable[Node], state: Progress)

  def breadthFirst(dlist: DList[(Node, NodeInfo)], depth: Int): DList[(Node, NodeInfo)] = {
    val firstMap = dlist.flatMap { case (n: Node, ni: NodeInfo) =>
      ni.state match {
        case Frontier(distance) =>
          List((n, NodeInfo(ni.edges, Done(distance)))) ++ ni.edges.map { edge => (edge, NodeInfo(List[Node](), Frontier(distance+1))) }
        case other              => List((n, NodeInfo(ni.edges, other)))
      }
    }

    val firstCombiner = firstMap.groupByKey.combine { (n1: NodeInfo, n2: NodeInfo) =>
      NodeInfo(if (n1.edges.isEmpty) n2.edges else n1.edges, furthest(n1.state, n2.state))
    }
    if (depth > 1) breadthFirst(firstCombiner, depth-1)
    else           firstCombiner
  }

  def furthest(p1: Progress, p2: Progress) = p1 match {
    case Unprocessed() => p2
    case Done(v1)      => Done(v1)
    case Frontier(v1)  => p2 match {
      case Unprocessed() => Frontier(v1)
      case Frontier(v2)  => Frontier(math.min(v1, v2))
      case Done(v2)      => Done(v2)
    }
  }

  def pathSize(p: Progress): Option[Int] = p match {
    case Frontier(v)   => Some(v)
    case Done(v)       => Some(v)
    case Unprocessed() => None
  }

  implicit object NodeComparator extends Ordering[Node] {
    def compare(a: Node, b: Node) = a.data compareTo b.data
  }


}


