package nl.tue.bdm;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

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


    // Write code in Spark to find the id of the song that was played the most times in the dataset. 
    // If more than one such song exists with an equal number of plays, return all of them.

    // Get tuples of (songID, 1)
    JavaPairRDD<Integer, Integer> songs = lines.mapToPair(x -> new Tuple2<Integer, Integer>(Integer.valueOf(x.split(",")[1]), 1));
    songs.take(10).forEach(System.out::println);

    // Add up the amount of times the song was listened to
    JavaPairRDD<Integer, Integer> songAmount = songs.reduceByKey((a,b) -> a + b);
    songAmount.take(10).forEach(System.out::println);

    // Get the max
    Integer maxAmount = songAmount.reduce((a,b) -> { if (a._2 > b._2) {return a;} else {return b;} })._2;
    System.out.println(maxAmount);

    // Get the values that match the max
    JavaPairRDD<Integer, Integer> result = songAmount.filter(x -> (x._2.equals(maxAmount)));
    result.take(10).forEach(System.out::println);

    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
    spark.stop();
  }
}
