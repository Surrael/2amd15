package nl.tue.bdm;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Main {
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .appName("2AMD15")
        .master("local[*]")
        .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();

    // Only lines with 3 elements contain a rating
    JavaRDD<String> linesWithRating = lines.filter(s -> s.split(",").length == 3);

    // We create pairs (userID, (1, rating)), with the 1 being used to keep track of
    // the amount of ratings a user did
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> pairs = linesWithRating.mapToPair(s -> {
      String[] words = s.split(",");
      Integer uid = Integer.valueOf(words[0]);
      Integer rating = Integer.valueOf(words[2]);
      return new Tuple2<>(uid, new Tuple2<>(1, rating));

    });

    // Sum the counts and the ratings for each user, so we have per userID:
    // (userID, countOfRatings, sumOfRatings)
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> countWithSumOfRatings = pairs
        .reduceByKey((pair1, pair2) -> {
          Integer count = pair1._1 + pair2._1;
          Integer rating = pair1._2 + pair2._2;
          return new Tuple2<>(count, rating);
        });

    // Compute avg rating per user: (userID, avgRating)
    JavaPairRDD<Integer, Integer> avgUserRatings = countWithSumOfRatings.mapValues(pair -> (pair._2 / pair._1));

    // Find the user with the highest avg rating
    Integer maxUser = avgUserRatings.reduce((u1, u2) -> u1._2 > u2._2 ? u1 : u2)._1;
    Integer maxAvgUserMinId = avgUserRatings.reduce((u1, u2) -> {
      if (u1._2 > u2._2)
        return u1;
      if (u2._2 > u1._2)
        return u2;
      return u1._1 < u2._1 ? u1 : u2; // if averages are equal, return the user with the smallest id
    })._1;

    System.out.println("---------------------------------------------");
    System.out.println("Max user (arbitrary tiebreaker): " + maxUser);
    System.out.println("Max user (min. id tiebreaker): " + maxAvgUserMinId);
    System.out.println("---------------------------------------------");

    spark.stop();
  }
}
