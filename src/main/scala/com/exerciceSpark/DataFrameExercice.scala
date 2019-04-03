package com.exerciceSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DataFrameExercice extends App {
  val sparkSession = SparkSession.builder.
    appName("WordCount").
    master("local[*]").
    getOrCreate()

  val csv_df = sparkSession.sqlContext.read.
    format("csv").option("header", "true").
    option("inferSchema", "true").
    load("src/main/assets/Netflix_2011_2016.csv")

//  csv_df.show()
//  csv_df.describe()

  val HVRatio = csv_df.withColumn("HV Ratio", csv_df("High")/csv_df("Volume"))

//  HVRatio.show()
  HVRatio.select(max("High")).show()
  HVRatio.select(avg("Close")).show()
  HVRatio.select(max("Volume"), min("Volume")).show()
  println(HVRatio.filter("Close < 600").count())


  val percent = (HVRatio.filter("High > 500").count()
    / HVRatio.count()) * 100
  println(percent)

//  Pearson correlation
  HVRatio.select(corr("High", "Volume")).
    show()

  val yeardf = HVRatio.
    withColumn("Year", year(HVRatio("Date")))
  val maxPerYear: Unit = yeardf.select("Year", "High").
    groupBy("Year").max().show()

  val monthdf = yeardf.
    withColumn("Month", month(yeardf("Date")))
  val monthAVG: Unit = monthdf.select("Month", "Close").
    groupBy("Month").mean().show()

}
