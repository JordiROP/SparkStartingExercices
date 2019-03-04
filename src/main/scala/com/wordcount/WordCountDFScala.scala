package com.wordcount

import org.apache.spark.sql.SparkSession

object WordCountDFScala extends App {
  val sparkSession = SparkSession.builder.
    appName("WordCount").
    master("local[*]").
    getOrCreate()

  val sc = sparkSession.sparkContext
  val rddText = sc.textFile("file:///C:/Users/jronrubia/Desktop/WordCountScala/src/main/assets/input.txt")
  import sparkSession.sqlContext.implicits._
  val count = rddText.flatMap(_.split(" |\\.")).
    toDF().
    groupBy("value").
    count()
  count.coalesce(1).write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("file:///C:/Users/jronrubia/Desktop/WordCountScala/src/main/assets/outputDFScala.csv")
}
