package scala.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object myschema {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)))
    val myRows = Seq(Row("Hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf.show()
  }
}
