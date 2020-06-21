package group.research.aging.spark.extensions
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

package object functions {
  def replace(str: String, sub: String = ""): UserDefinedFunction = udf[String, String]{ v =>  v.replace(str, sub)}
  def undot: UserDefinedFunction =  udf[String, String]{ str=> str.indexOf(".") match { case -1 => str; case i => str.substring(0, i)} }
  def coord(i: Int): UserDefinedFunction = udf[Double, org.apache.spark.ml.linalg.Vector]{ vec => vec(i) }
  //def get_subject: UserDefinedFunction =  udf[String, String]{ str=> str.split("-").take(2).mkString("-")}
  //def get_avg_age: UserDefinedFunction =  udf[Double, String]{ str=> val arr = str.split("-"); (arr(1).toInt + arr(0).toInt) / 2.0}

}
