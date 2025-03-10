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

    Dataset<Row> df = spark.read().csv("plays.csv")
      .toDF("user_id", "song_id");        
    df.show(10);

    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();
    lines.take(10).forEach(System.out::println);

    // Write code in Spark to find the id of the song that was played the most times in the dataset. 
    // If more than one such song exists with an equal number of plays, return all of them.

    df.createOrReplaceTempView("plays");
    Dataset<Row> songCounts = spark.sql(
        "SELECT song_id, COUNT(song_id) as count " +
            "FROM plays " +
            "GROUP BY song_id ");
    songCounts.show(10);

    songCounts.createOrReplaceTempView("countedPlays");

    Dataset<Row> countMax = spark.sql(
        "SELECT MAX(count) as max " +
            "FROM countedPlays " );
    countMax.show(1);

    Long maxValue = (Long) countMax.collectAsList().get(0).get(0);    
    System.out.println(maxValue);

    // Get the values that match the max
    Dataset<Row> maxSongs = spark.sql(
        "SELECT song_id, count " +
            "FROM countedPlays " +
            "WHERE count == " + maxValue );
            maxSongs.show(10);

    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
    spark.stop();
  }
}
