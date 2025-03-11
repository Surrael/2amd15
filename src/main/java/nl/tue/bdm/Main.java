package nl.tue.bdm;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Main {
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .appName("2AMD15")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().csv("plays.csv");
    df.show(10);

    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();
    lines.take(10).forEach(System.out::println);

    /***
     * You want to build a recommendation model with Spark, for recommending new songs to users based on their ratings on past songs. 
     * There are many ways to answer this question, as well as many definitions on what would constitute a good recommendation. 
     * You are free to choose, and experiment with different ones. One possible approach is to model the users/songs ratings as a sparse matrix, 
     * and then run classical data mining/machine learning algorithms, such as K-Means or K-nearest neighbor, to find possible answers. 
     * Mllib is a Spark library that can be used for this exercise.
    ***/


    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
    spark.stop();
  }
}
