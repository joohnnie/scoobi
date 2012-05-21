package com.nicta.scoobi.lib

import com.nicta.scoobi.Scoobi._
import scala.collection.mutable.ArrayBuffer
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.DObject
import com.nicta.scoobi.DObject._

object DMatrix {
  implicit def dlistToMatrix[Elem: Manifest: WireFormat: Ordering, Value: Manifest: WireFormat](v: DList[((Elem, Elem), Value)]): DMatrix[Elem, Value] = DMatrix[Elem, Value](v)

  implicit def matrixToDlist[Elem: Manifest: WireFormat: Ordering, Value: Manifest: WireFormat](v: DMatrix[Elem, Value]): DList[((Elem, Elem), Value)] = v.m
}

case class DMatrix[Elem: Manifest: WireFormat: Ordering, Value: Manifest: WireFormat](m: DList[((Elem, Elem), Value)]) {

  def byMatrix[V: Manifest: WireFormat, Q: Manifest: WireFormat](
    n: DMatrix[Elem, V],
    mult: (Value, V) => Q,
    add: (Q, Q) => Q): DMatrix[Elem, Q] =
    {
      val left = m.by(_._1._2).map(x => (x._1, Left(x._2): Either[((Elem, Elem), Value), ((Elem, Elem), V)]))
      val right = n.m.by(_._1._1).map(x => (x._1, Right(x._2): Either[((Elem, Elem), Value), ((Elem, Elem), V)]))

      (left ++ right).groupByKey.parallelDo(DObject(()),
        new BasicDoFn[(Elem, Iterable[Either[((Elem, Elem), Value), ((Elem, Elem), V)]]), ((Elem, Elem), Q)] {
          def process(input: (Elem, Iterable[Either[((Elem, Elem), Value), ((Elem, Elem), V)]]), emitter: Emitter[((Elem, Elem), Q)]) = {
            val as: ArrayBuffer[((Elem, Elem), Value)] = new ArrayBuffer[((Elem, Elem), Value)]()
            val bs: ArrayBuffer[((Elem, Elem), V)] = new ArrayBuffer[((Elem, Elem), V)]()

            input._2 foreach {
              case Left(a) => {
                as += a
                bs.foreach {
                  b => emitter.emit((a._1._1, b._1._2), mult(a._2, b._2))
                }
              }
              case Right(b) => {
                bs += b
                as.foreach {
                  a => emitter.emit((a._1._1, b._1._2), mult(a._2, b._2))
                }
              }
            }
          }
          def cleanup(emitter: Emitter[((Elem, Elem), Q)]) = {}
        }).groupByKey.combine((a: Q, b: Q) => add(a, b))
    }

  // work around a hadoop bug with combiners timing out...
  def byMatrixWithoutCombiner[V: Manifest: WireFormat, Q: Manifest: WireFormat](
    n: DMatrix[Elem, V],
    mult: (Value, V) => Q,
    add: (Q, Q) => Q): DMatrix[Elem, Q] =
    Join.join(m.by(_._1._2), n.m.by(_._1._1))
      .map(_._2)
      .map { case (a, b) => ((a._1._1, b._1._2), mult(a._2, b._2)) }
      .groupByKey
      .combine((a: Q, b: Q) => add(a, b))

}

object DenseLinear {
  type DenseVector[Elem, T] = DObject[Map[Elem, T]]

  type WiseMatrix[Elem, T] = DList[(Elem, Iterable[(Elem, T)])]

  type RowWiseMatrix[Elem, T] = WiseMatrix[Elem, T]
  type ColWiseMatrix[Elem, T] = WiseMatrix[Elem, T]

  def toRowWiseMatrix[T: Manifest: WireFormat](dm: DMatrix[Int, T]): RowWiseMatrix[Int, T] =
    dm.map { case ((r, c), v) => (r, (c, v)) }.groupByKey

  def toColWiseMatrix[T: Manifest: WireFormat](dm: DMatrix[Int, T]): ColWiseMatrix[Int, T] =
    dm.map { case ((r, c), v) => (c, (r, v)) }.groupByKey

  def toDenseVector[T: WireFormat: Manifest](in: DList[(Int, T)], zero: T): DenseVector[Int, T] = in.materialize.map(xs => xs.toMap)

  def matrixByVector[T: Manifest: WireFormat, V: Manifest: WireFormat, R: Manifest: WireFormat](m: RowWiseMatrix[Int, T],
    dv: DenseVector[Int, V],
    zero: R,
    mult: (T, V) => R,
    add: (R, R) => R): DenseVector[Int, R] = {

    val all = dv join m
    
    val distributedVector =
      all.map {
        case (arr, (elem, vals)) => {

          val products =
            for (q <- vals if arr.contains(q._1))
              yield mult(q._2, arr(q._1))

          val result = if (products.isEmpty) zero else products.reduce(add)

          (elem, result)
        }
      }

    toDenseVector(distributedVector, zero)
  }

  def vectorByMatrix[T: Manifest: WireFormat, V: Manifest: WireFormat, R: Manifest: WireFormat](dv: DenseVector[Int,V], m: ColWiseMatrix[Int, T],
    zero: R,
    mult: (V, T) => R,
    add: (R, R) => R): DenseVector[Int, R] = matrixByVector(m, dv, zero, (a: T, b: V) => mult(b, a), add)
}



