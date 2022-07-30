package scala.practice

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{asc, col, column, desc, expr, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.Metadata

import scala.practice.DataSet.getClass

object struct {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

//    val df1 = spark.read.json("data/flight-data/json/2015-summary.json")
//
//    df1.printSchema()

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json").schema(myManualSchema)
      .load("data/flight-data/json/2015-summary.json")

    df.printSchema()

    df.createOrReplaceTempView("dfTable")

    df.select(
      col("*"),
      df.col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME")
    ).show()

    df.selectExpr(
      "*",
      "DEST_COUNTRY_NAME as newCountry",
      "ORIGIN_COUNTRY_NAME as oldCountry",
      "ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME as withinCountry", // boolean
    ).show(100)

    df.selectExpr(
      "avg(count)",
      "count(distinct(DEST_COUNTRY_NAME))"
    ).show(5)

    df.select(
      expr("*"),
      lit(1).as("One")
    ).show(5)

    df.withColumn("newColumn", expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME")).show(20)
    df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(2)

    df.withColumn("count2", col("count").cast("string")).show(2)

    df.select(
      "ORIGIN_COUNTRY_NAME",
      "DEST_COUNTRY_NAME")
      .distinct().count()

    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)
    df.where("count < 2")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

    val seed = 5
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    dataFrames(0).count() > dataFrames(1).count() // False

    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema)
    df.union(newDF)
      .where("count = 1")
      .where(col("ORIGIN_COUNTRY_NAME") =!= col("United States"))
      .show() // get all of them and we'll see our new rows at the end

    df.sort("count").limit(100).show(10)
    df.orderBy(desc("count"), asc("ORIGIN_COUNTRY_NAME"))

    println(df.rdd.getNumPartitions)

  }
}
