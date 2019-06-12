package group.research.aging.spark.extensions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ColumnName, Dataset}
import org.apache.spark.sql.expressions.Window

trait ReadExtensions {

  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

  import scala.reflect.runtime.universe._

  def pathExists(path: String): Boolean = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hdfs.exists( new Path(path))
  }

  implicit class SparkSessionExtended(session: SparkSession)  extends HDFSFilesExtensions {

    import session.implicits._

    def sparkContext: SparkContext = session.sparkContext

    def readTSV(path: String, headers: Boolean = false, sep: String = "\t", comment: String = "#", sqlName: String = ""): DataFrame = {
      val df = session.read
        .option("sep", sep)
        .option("comment", comment)
        .option("inferSchema", true)
        .option("header", headers)
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("maxColumns", 150000)
        .csv(path)
      if(sqlName!="") df.createOrReplaceTempView(sqlName)
      df
    }

    def readTypedTSV[T <: Product](path: String, header: Boolean = false, sep: String = "\t", sqlName: String = "")
                                  (implicit tag: TypeTag[T]): Dataset[T] = {
      implicit val encoder: StructType = Encoders.product[T](tag).schema
      val df = session.read
        .option("sep", sep)
        .option("comment", "#")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .option("header", header)
        .schema(encoder)
        .csv(path).as[T]
      if(sqlName!="") df.createOrReplaceTempView(sqlName)
      df
    }

    def readParquet(path: String, sqlName: String = ""): DataFrame = {
      val df = session.read.parquet(sqlName)
      if(sqlName!="") df.createOrReplaceTempView(sqlName)
      df
    }

    def tsv2parquet(path: String, parquetPath: String, headers: Boolean = true, sep: String = "\t", comment: String = "#", loadIfExist: Boolean = true): DataFrame = {
      if(pathExists(parquetPath) && loadIfExist) {
        session.read.parquet(parquetPath)
      } else {
        val df = readTSV(path, headers, sep, comment)
        df.write.parquet(parquetPath)
        df
      }

    }

    def parquet2tsv(parquetPath: String, tsvPath: String, headers: Boolean = true, sep: String = "\t") = {
      val df = session.read.parquet(parquetPath)
      df.writeTSV(tsvPath, headers, sep)
      df
    }

    def rank(df: DataFrame, name: String, rankSuffix: String = "_rank"): DataFrame =
      df.withColumn(name + rankSuffix, org.apache.spark.sql.functions.dense_rank().over(Window.orderBy(new ColumnName(name).desc)))

    def ranks(df: DataFrame,
              names: Seq[String],
              rankSuffix: String = "_rank"): DataFrame = names.foldLeft(df){
      case (f, n)=> rank(f, n, rankSuffix)
    }



  }
}
