package group.research.aging.spark.extensions

import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.immutable


trait Local {
  import ammonite.ops._
 // import ammonite.ops.ImplicitWd._

  def name(p: Path) = if(p.ext!="") p.baseName + "." + p.ext else p.baseName

  def movePart(dir: Path, part: Path, extension: String = "") = { dir.toIO.exists()
    val newPlace = dir / up / name(part)
    mv(part, newPlace)
    val dirName = if(extension == "" || dir.ext == extension) name(dir) else name(dir).replace(dir.ext, extension)
    val newName = dir / up / dirName
    rm! dir
    mv(newPlace, newName)
  }

  def mergeParts(dir: Path, extension: String = "") = {
    val parts = (ls! dir).filter(p=> !(name(p).startsWith(".")) && name(p).contains("part"))
    if(parts.size > 1) println(s"merging multiple parts of ${dir} is not yet supported, please resave stuff with coalescence(1)\n Parts are: ${parts.mkString("\n")}")
    else if(parts.isEmpty) println("no parts detected!")
    else {
      movePart(dir, parts.head, extension)
      println(s"parts of ${dir} merged!")
    }
  }

}

trait DataFrameExtensions extends Local {

  import org.apache.spark._
  import org.apache.spark.sql.types.StructType
  import scala.reflect.runtime.universe._
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.rdd._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import group.research.aging.spark.extensions._
  import group.research.aging.spark.extensions.functions._

  implicit class DataFrameExtended(dataFrame: DataFrame) {

    //adds index to make it easier to join the dataframes
    def withIndex(implicit sparkSession: SparkSession): DataFrame = {
      val ind = StructType(dataFrame.schema.fields :+ StructField("_index", LongType, nullable = false))
      val zp =  dataFrame.rdd.zipWithIndex.map{case (r, i) => Row.fromSeq(r.toSeq :+ i)}
      sparkSession.createDataFrame(zp, ind)
    }

    /**
      * Transposes a dataframe
      * @param transBy
      * @param sparkSession
      * @return
      */
    def transposeUDF(transBy: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {
      import sparkSession.implicits._
      val (cols, types) = dataFrame.dtypes.filter{ case (c, _) => !transBy.contains(c)}.unzip
      require(types.distinct.length == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("column_name"), col(c).alias("column_value"))): _*
      ))

      val byExprs = transBy.map(col)

     dataFrame
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.column_name", $"_kvs.column_value"): _*)
    }

    /**
      * Writes dataframe as TSV file
      * @param path
      * @param header
      * @param sep
      * @return
      */
    def writeTSV(path: String, header: Boolean = true, sep: String = "\t", local: Boolean = false): String =
    {
      val df = if(local) dataFrame.coalesce(1) else dataFrame
      df.write.option("sep", sep).option("header",header).option("maxColumns", 150000).csv(path)
      if(local) {
        //println("merging is not yet implemented!")
        mergeParts(ammonite.ops.Path(path))
      }
      path
    }

    def rename(renamings: Map[String, String]): DataFrame =   {
      val newColumns = dataFrame.columns.map(c=> renamings.getOrElse(c, c))
      dataFrame.toDF(newColumns:_*)
    }

    def joinWithOthers(dfs: List[DataFrame], fields: Seq[String], joinType: String = "inner"): DataFrame = {
      (dataFrame::dfs).reduce((a, b)=> a.join(b, fields, joinType))
    }

    def rank(name: String, rankSuffix: String = "_rank"): DataFrame =
      dataFrame.withColumn(name + rankSuffix, org.apache.spark.sql.functions.dense_rank().over(Window.orderBy(new ColumnName(name).desc)))

    def ranks(names: Seq[String], rankSuffix: String = "_rank"): DataFrame = names.foldLeft(dataFrame){
      case (f, n)=> f.rank(n, rankSuffix)
    }

    def toVectors(columns: Seq[String], output: String): DataFrame = {
      import org.apache.spark.ml.feature.VectorAssembler

      val assembler = new VectorAssembler()
        .setInputCols(columns.toArray)
        .setOutputCol(output)

      assembler.transform(dataFrame.na.fill(0.0, columns).na.fill(0.0)).select(output)
    }

    import org.apache.spark.ml.feature.PCA
    def fitPCA(columns: Seq[String], k: Int)(implicit sparkSession: SparkSession): PCAModel = {
      val df = dataFrame.toVectors(columns, "features")
      new PCA()
        .setInputCol("features")
        .setOutputCol("PCA")
        .setK(k)
        .fit(df)
    }


    def doPCA(columns: Seq[String], k: Int)(implicit sparkSession: SparkSession): DataFrame = {
      val vec = dataFrame.toVectors(columns, "features")
      val pca = new PCA()
        .setInputCol("features")
        .setOutputCol("PCA")
        .setK(k)
        .fit(vec)
      pca.transform(vec)
    }


    protected def convertCorrellationMatrix(matrix: Matrix, columns: Seq[String]): immutable.IndexedSeq[Row] = {
      require(columns.size == matrix.numCols)
      for(r <- 0 until matrix.numRows) yield {
        val seq = for(c <- 0 until matrix.numCols) yield matrix(r, c)
        Row.fromSeq(columns(r)::seq.toList)
      }
    }

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    def reduceByMax(key: String, maximize: String)(implicit sparkSession: SparkSession): DataFrame = {
      import sparkSession.implicits._
      val schema = dataFrame.schema
      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      val encoder = RowEncoder(schema)
      dataFrame.groupByKey(row=>row.getAs[String](key)).reduceGroups {
        (a, b) => if(a.getAs[Double](maximize) > b.getAs[Double](maximize)) a else b
      }.map(_._2)(encoder)
    }

    def reduceByMin(key: String, minimize: String)(implicit sparkSession: SparkSession): DataFrame = {
      import sparkSession.implicits._
      val schema = dataFrame.schema
      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      val encoder = RowEncoder(schema)
      dataFrame.groupByKey(row=>row.getAs[String](key)).reduceGroups {
        (a, b) => if(a.getAs[Double](minimize) < b.getAs[Double](minimize)) a else b
      }.map(_._2)(encoder)
    }

    def doublesByColumns(columns: Seq[String]): List[StructField] = columns.map(c=>StructField(c, DoubleType, false)).toList

    def transformCorrellationMatrix(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {
      val rows  = dataFrame.rdd
        .flatMap{ case Row(matrix: Matrix) => convertCorrellationMatrix(matrix, columns) }
      sparkSession.createDataFrame(rows, StructType(StructField("column", StringType, false)::doublesByColumns(columns)))
    }

    def pearsonCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {
      val cor = dataFrame.toVectors(columns.toSeq, "features")
      val df = Correlation.corr(cor, "features")
      transformCorrellationMatrix(df, columns)
    }


    def spearmanCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame = {
      val cor = dataFrame.toVectors(columns.toSeq, "features").persist(StorageLevel.MEMORY_AND_DISK)
      val df = Correlation.corr(cor, "features", method = "spearman")
      transformCorrellationMatrix(df, columns)
    }


  }
  import cats._
  import cats.implicits._

  implicit val dataFrameSemigroup: Semigroup[DataFrame] = new Semigroup[DataFrame] {
    def combine(x: DataFrame, y: DataFrame): DataFrame = if(x.columns.toSet == y.columns.toSet) x.union(y) else x.join(y, x.columns.intersect(y.columns))
  }

  /*
  def same(mp: Map[String, DataFrame]): DataFrame = mp.values.reduce{ case (a, b) => a.join(b, Seq("gene"))}

  def unique(mp: Map[String, DataFrame], key: String): DataFrame = {
    val list: List[DataFrame] = mp.filterKeys(_!=key).values.toList
    val el:DataFrame = mp(key)
    val df: DataFrame = (el::list).reduce{ (a, b) => a.join(b, Seq("gene"), "leftanti")}
    df.sort(new ColumnName(key + "p_value"), new ColumnName(key + "_log2(fold_change)").desc)
  }
  */

}
