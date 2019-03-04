package com.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountRDDJava {
    public static void main(String[] args) {

        SparkConf sc = new SparkConf().
                setMaster("local[*]").
                setAppName("WordCount RDD Java");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> rddFile = jsc.textFile(
                "file:///C:/Users/jronrubia/Desktop/WordCountSpark/src/main/assets/input.txt",
                1);

        JavaPairRDD<String, Integer> counts = rddFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(
                "file:///C:/Users/jronrubia/Desktop/WordCountSpark/src/main/assets/outputRDDJava.txt");
    }
}

