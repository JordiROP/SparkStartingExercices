package com.wordcount

import org.apache.spark.sql.SparkSession

object WordCountRDDScala extends App {

  val sparkSession = SparkSession.builder.
    appName("WordCount").
    master("local[*]").
    getOrCreate()

  val sc = sparkSession.sparkContext
  val rddText = sc.textFile("file:///C:/Users/jronrubia/Desktop/WordCountScala/src/main/assets/input.txt")
  val count = rddText.flatMap(l => l.split(" |\\.")).
    map(w => (w, 1)).
    reduceByKey(_ + _)

  val singleCount = count.coalesce(1)
  singleCount.saveAsTextFile("file:///C:/Users/jronrubia/Desktop/WordCountScala/src/main/assets/outputRDDScala.txt")
}
