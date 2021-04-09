package group.research.aging.spark.extensions.functions

/**
  * Aggregation function to concatenate string content of columns
  * @param delimiter
  */
class ConcatenateString(delimiter: String) extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.types._

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

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val value = buffer.get(0)
    val str =  input.getString(0)
    buffer.update(0, if(value=="" || value == null) str else value + delimiter  + str)
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val str =  buffer2.getString(0)
    buffer1.update(0, buffer1.getString(0) +  str)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row): String = {
    buffer.getString(0)
  }
}
