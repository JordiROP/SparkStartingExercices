package com.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WordCountDFSJava {

    SparkConf sc = new SparkConf().
            setMaster("local[*]").
            setAppName("WordCount RDD Java");
    JavaSparkContext jsc = new JavaSparkContext(sc);

    JavaRDD<String> rddFile = jsc.textFile(
            "file:///C:/Users/jronrubia/Desktop/WordCountSpark/src/main/assets/input.txt",
            1);
}
