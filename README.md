# spark-extensions
Various extensions for Spark

DataFrame extensions
--------------------

It adds some useful extension methods to DataFrame type, like:
```scala
//general functions
def writeTSV(path: String, header: Boolean = true, sep: String = "\t"): String
def rename(renamings: Map[String, String]): DataFrame
def joinWithOthers(dfs: List[DataFrame], fields: Seq[String], joinType: String = "inner"): DataFrame 

//aggregate functions
def reduceByMax(key: String, maximize: String)(implicit sparkSession: SparkSession): Dataset[Row]
def reduceByMin(key: String, minimize: String)(implicit sparkSession: SparkSession): Dataset[Row]

//ML functions
def toVectors(columns: Seq[String], output: String): DataFrame
def ranks(names: Seq[String], rankSuffix: String = "_rank"): DataFrame
def fitPCA(columns: Seq[String], k: Int)(implicit sparkSession: SparkSession): PCAModel 

//correlations
def pearsonCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame
def spearmanCorrellation(columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame
def transformCorrellationMatrix(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession): DataFrame 
```

SparkSession extensions
------------------------
```scala
 def readTSV(path: String, headers: Boolean = false, sep: String = "\t", comment: String = "#"): DataFrame
 def readTypedTSV[T <: Product](path: String, header: Boolean = false, sep: String = "\t")(implicit tag: TypeTag[T]): Dataset[T]     
```

Additional aggregate functions
--------------------

```scala
class ConcatenateString(delimiter: String) extends UserDefinedAggregateFunction
```


Adding to dependencies
=======================

Add this to your Apache Zeppelin configuration:
```
%spark.dep

z.addRepo("combioaging").url("https://dl.bintray.com/comp-bio-aging/main")
z.load("group.research.aging:spark-extensions_2.11:0.0.2")
```