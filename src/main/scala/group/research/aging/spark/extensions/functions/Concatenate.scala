package group.research.aging.spark.extensions.functions

import scala.collection.mutable.ListBuffer


class Concatenate(delimiter: String, unique: Boolean) extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.expressions._

  // Input Data Type Schema
  def inputSchema: org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType(
    Array(StructField("value", org.apache.spark.sql.types.StringType))
  )

  // Intermediate Schema
  def bufferSchema = StructType(
    Array(
      StructField("values", ArrayType(StringType, false))
    )
  )


  // Returned Data Type .
  def dataType = StringType

  // Self-explaining
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array.empty[String])
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val str =  input.getString(0)
    val arr = buffer.getSeq[String](0)
    unique match {
      case true if !arr.contains(str) =>
        val newValue = arr :+ str
        buffer.update(0, newValue)
      case false =>
        val newValue = arr :+ str
        buffer.update(0, newValue)
      case _ => //do nothing
    }
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val one =  buffer1.getSeq[String](0)
    val two = buffer2.getSeq[String](0)
    val merged= if(unique) (one ++ two).distinct else one ++ two
    buffer1.update(0, merged)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row): String = {
    val seq = buffer.getSeq[String](0)
    seq.mkString(delimiter)
  }
}

