package group.research.aging.spark

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

/**
  * Extensions for standard ADAM classes
  */
package object extensions extends  DataFrameExtensions with ReadExtensions {

    def joinDataFrames(dfs: Seq[DataFrame], fields: Seq[String], joinType: String = "inner"): DataFrame = {
      dfs.reduce((a, b)=> a.join(b, fields, joinType))
    }

}
