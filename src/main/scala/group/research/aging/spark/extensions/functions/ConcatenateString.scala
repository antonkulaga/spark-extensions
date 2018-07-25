package group.research.aging.spark.extensions.functions

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

class ConcatenateString(delimiter: String) extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
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
    Array(StructField("value", StringType))
  )

  // Returned Data Type .
  def dataType = StringType

  // Self-explaining
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, "")
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val str =  input.getString(0)
    buffer.update(0, buffer.get(0)  + str+ delimiter)
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val str =  buffer2.getString(0)
    buffer1.update(0, buffer1.getString(0) +  str)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getString(0)
  }
}