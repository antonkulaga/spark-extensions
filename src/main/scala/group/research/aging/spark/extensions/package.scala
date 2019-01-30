package group.research.aging.spark

import org.apache.spark.sql.DataFrame

/**
  * Extensions for standard ADAM classes
  */
package object extensions extends  DataFrameExtensions with ReadExtensions {

    def joinDataFrames(dfs: Seq[DataFrame], fields: Seq[String], joinType: String = "inner"): DataFrame = {
      dfs.reduce((a, b)=> a.join(b, fields, joinType))
    }

}
