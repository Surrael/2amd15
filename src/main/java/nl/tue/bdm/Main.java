package nl.tue.bdm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .appName("2AMD15")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().csv("plays.csv")
        .toDF("user_id", "song_id");

    // Find the counts of all songs and order by descending count
    df.createOrReplaceTempView("df");
    Dataset<Row> songCounts = spark.sql(
        "SELECT song_id, COUNT(song_id) as count " +
            "FROM df " +
            "GROUP BY song_id " +
            "ORDER BY count DESC");

    Long maxCount = songCounts.collectAsList().get(0).getLong(1);

    // Find all songs that have the maximum count
    songCounts.createOrReplaceTempView("songCounts");
    Dataset<Row> mostPlayedSongs = spark.sql(
        "SELECT song_id " +
            "FROM songCounts " +
            "WHERE count = " + maxCount);

    mostPlayedSongs.show();
    spark.stop();
  }
}
