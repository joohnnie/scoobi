/**
 * Copyright 2011 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi.examples

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.lib.DenseLinear._
import com.nicta.scoobi.lib.DMatrix
import com.nicta.scoobi.lib.DMatrix._
import scala.util.Random
import java.io.FileWriter
import scala.collection.immutable.HashSet
import scala.sys.process._
import com.nicta.scoobi.ScoobiConfiguration
import org.apache.commons.math.linear.SparseFieldMatrix
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.math.util.BigReal
import org.apache.commons.math.util.BigRealField
import org.apache.commons.math.linear.FieldMatrixPreservingVisitor
import org.apache.commons.math.linear.FieldMatrix

object LinearAlgebraTest {

  def main(a: Array[String]) = {

    implicit val conf = ScoobiConfiguration()

    conf.withHadoopArgs(a) { args =>
     
      val matrix1Data = generateDataSet()

      matrix1Data.map(println)

      val matrix2Data = generateDataSet()

      val dm1 = dmatrixFromData(matrix1Data)
      val am1 = apacheMatrixFromData(matrix1Data)


      val dm2 = dmatrixFromData(matrix2Data)
      val am2 = apacheMatrixFromData(matrix2Data)

      // normal matrix multiplication
      val oneByTwo = toMatrixEntrySet(dm1.byMatrix(dm2, mult, add))
      
      // matrix multiplication to work around hadoop bug
      val oneByTwo2 = toMatrixEntrySet(dm1.byMatrixWithoutCombiner(dm2, mult, add))

      // apache reference multiplication
      val oneByTwo3 = toMatrixEntrySet(am1.multiply(am2))

      println(oneByTwo == oneByTwo2, " and ", oneByTwo2 == oneByTwo3)

    }
  }

  case class MatrixEntry(row: Int, col: Int, value: Int)

  def toMatrixEntrySet(m: DMatrix[Int, Int])(implicit conf: ScoobiConfiguration) =
    Set[MatrixEntry]() ++ persist(m.materialize).collect { case ((r, c), v) if v != 0 => MatrixEntry(r, c, v) }

  def toMatrixEntrySet(m: FieldMatrix[BigReal]): Set[MatrixEntry] = {

    class SetAdder(var ms: Set[MatrixEntry] = Set[MatrixEntry]()) extends FieldMatrixPreservingVisitor[BigReal] {
      def start(rows: Int, columns: Int, startRows: Int, endRow: Int, startColumn: Int, endColumn: Int) {}
      def end(): BigReal = null
      def visit(row: Int, col: Int, value: BigReal) = {

        val v = value.bigDecimalValue.intValueExact

        if (v != 0)
          ms = ms + MatrixEntry(row, col, v)
      }
    }

    val sa = new SetAdder()

    m.walkInOptimizedOrder(sa)

    sa.ms
  }

  def dmatrixFromData(d: Array[MatrixEntry]): DMatrix[Int, Int] = {

    val fn = "temp-test-" + rand.nextInt

    val fw = new FileWriter(fn)

    d.foreach(me => {
      fw.write(me.row + "," + me.col + "," + me.value + "\n")
    })

    fw.close()

    fromDelimitedTextFile(fn, ",") {
      case AnInt(r) :: AnInt(c) :: AnInt(v) :: _ => ((r, c), v)
    }

   // DList(d: _*).map(q => ((q.row, q.col), q.value))
  }

  def apacheMatrixFromData(d: Array[MatrixEntry]): SparseFieldMatrix[BigReal] = {

    val sfm = new SparseFieldMatrix[BigReal](BigRealField.getInstance, 100, 100)

    d.foreach { q =>
      sfm.setEntry(q.row, q.col, new BigReal(q.value))
    }

    sfm
  }

  def mult(x: Int, y: Int) = x * y
  def add(x: Int, y: Int) = x + y
  val zero = 0

  private val rand = new Random()

  private def generateDataSet(): Array[MatrixEntry] = {

    var usedValues = new HashSet[(Int, Int)]()

    val arr = ArrayBuffer[MatrixEntry]()

    var r = 0
    var c = 0;

    for (_ <- 1 to rand.nextInt(100) + 5) {

      do {
        r = rand.nextInt(30)
        c = rand.nextInt(30)
      } while (usedValues.contains((r, c)))

      usedValues = usedValues + ((r, c))

      arr += MatrixEntry(r, c, rand.nextInt(10))
    }

    arr.toArray
  }

  private def generateVectorSet(rowWise: Boolean, path: String) {
    val e = new FileWriter(path)

    var usedValues = new HashSet[Int]()
    var x = 0

    for (_ <- 1 to rand.nextInt(500) + 30) {
      do {
        x = rand.nextInt(1000)
      } while (usedValues.contains(x))

      usedValues = usedValues + x

      if (rowWise)
        e.write("0," + x)
      else
        e.write(x + ",0")

      e.write("," + rand.nextInt(100) + "\n")
    }

    e.close()
  }
}
