package group.research.aging.spark.extensions

/*
object Test {
  import org.apache.spark
  import org.apache.spark._

  import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
  import org.apache.spark.sql.types.StructType
  import scala.reflect.runtime.universe._
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.rdd._
  import org.apache.spark.sql.functions._
  import group.research.aging.spark.extensions._
  import org.apache.spark.sql.expressions._
  import group.research.aging.spark.extensions._
  import group.research.aging.spark.extensions.functions._
  val sparkContext = sc
  val spark = SparkSession.builder().appName("species")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //.config("spark.kryo.referenceTracking", true)
    .config("spark.dynamicAllocation.minExecutors", 1)
    .getOrCreate()
  val session = spark

  val index = spark.readTSV("/data/samples/index.tsv", headers = true)
  index.filter($"organism" === "maca")
}
*/