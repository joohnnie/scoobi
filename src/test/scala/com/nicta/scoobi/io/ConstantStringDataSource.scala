package com.nicta.scoobi
package io

import java.util.Arrays._
import org.apache.hadoop.mapreduce._
import lib.input.InvalidInputException
import org.apache.hadoop.filecache.DistributedCache

import application.ScoobiConfiguration

class ConstantStringDataSource(value: String) extends DataSource[String, String, String] {

  def inputFormat: Class[_ <: InputFormat[String, String]] = classOf[ConstantStringInputFormat]
  def inputCheck(sc: ScoobiConfiguration) {}
  def inputConfigure(job: Job) {
    job.getConfiguration.set("mapred.constant.string", "value")
    DistributedCache.addCacheFile(new java.net.URI("string"), job.getConfiguration)
  }
  def inputSize: Long = value.size
  lazy val inputConverter = ConstantStringInputConverter(value)

  case class ConstantStringInputSplit(value: String) extends InputSplit {
    def getLength = value.size
    def getLocations = Array[String]()
  }

  case class ConstantStringInputConverter(value: String) extends InputConverter[String, String, String] {
    def fromKeyValue(context: this.type#InputContext, key: String, v: String) = value
  }
}
object ConstantStringDataSource {
  def apply(value: String) = new ConstantStringDataSource(value)
}
class FailingDataSource extends ConstantStringDataSource("") {
  override def inputFormat: Class[_ <: InputFormat[String, String]] = classOf[FailingInputFormat]
}
object FailingDataSource {
  def apply() = new FailingDataSource
}
case class ConstantStringRecordReader(value: String) extends RecordReader[String, String] {
  def this() = this("")
  def initialize(split: InputSplit, context: TaskAttemptContext) {}
  def nextKeyValue() = false
  def getCurrentKey = value
  def getCurrentValue = value
  def getProgress = 0.0f
  def close() {}
}

class ConstantStringInputFormat(value: String) extends InputFormat[String, String] {
  def this() = this("")
  def getSplits(context: JobContext) = asList(ConstantStringInputSplit(value))
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = ConstantStringRecordReader(value)
}

class FailingInputFormat extends InputFormat[String, String] {
  def getSplits(context: JobContext) = { throw new InvalidInputException(asList()); asList() }
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = ConstantStringRecordReader("")
}

case class ConstantStringInputSplit(value: String) extends InputSplit {
  def getLength = value.size
  def getLocations = Array("localhost")
}


