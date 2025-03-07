package nl.tue.bdm;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Main {
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .appName("2AMD15")
        .master("local[*]")
        .getOrCreate();

    // QUESTION 1

    // Dataframe
    Dataset<Row> df = spark.read().csv("plays.csv");
    df.show(10);

    // RDD
    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();
    lines.take(10).forEach(System.out::println);

    // QUESTION 2
    /**Write code in Spark to find the id of the user that gave at least 10 ratings, and has the highest average rating. 
     * If more than one such users exist with equal average rating, you can break the ties arbitrarily (i.e., return any one of them). 
     * You are not allowed to use SparkSQL or dataframes/datasets for this exercise. 
     * You are only allowed to use RDDs. The format of the answer should be a pair of <id, averageRating>  **/

    // Get only the lines with ratings 
    JavaRDD<String> onlyRatings = lines.filter(s -> s.split(",").length == 3);
    onlyRatings.take(10).forEach(System.out::println);

    // Create pairs for the keys, where the values are
    //    the a pair of 1 and the rating
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratingPairs = onlyRatings.mapToPair(s -> 
          new Tuple2<>(Integer.parseInt(s.split(",")[0]), new Tuple2<>(1, Integer.parseInt(s.split(",")[2]))));
    ratingPairs.take(10).forEach(System.out::println);

    // Create pairs for the keys, where the values are
    //    the a pair of #ratings and the rating
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratingsCount = ratingPairs.reduceByKey((l, r) -> 
      new Tuple2<>(l._1 + r._1, l._2 + r._2));
    ratingsCount.take(10).forEach(System.out::println);

    // Filter users with more than 10 ratings
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratingsCountFiltered = ratingsCount.filter(s -> s._2._1 >= 10);
    ratingsCountFiltered.take(10).forEach(System.out::println);

    // Get the average ratings
    JavaPairRDD<Integer, Float> averageRatings = ratingsCountFiltered.mapValues(s -> s._2 / (float)s._1);
    averageRatings.take(10).forEach(System.out::println);

    // Get the highest average rating
    Tuple2<Integer, Float> highestAverageRating = averageRatings.reduce((x ,y) -> {if (x._2 >= y._2) {return x;} else {return y;}});
    System.out.println(highestAverageRating);

    // For me :)
    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    spark.stop();
  }
}
