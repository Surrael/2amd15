package nl.tue.bdm;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

    // Read the file
    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();
    lines.take(10).forEach(System.out::println);

    // Only get the IDs
    JavaRDD<Integer> songIDs = lines.map(line -> Integer.valueOf(line.split(",")[1].trim()));
    songIDs.take(10).forEach(System.out::println);

    // Initalize rows/columns and the hash functions for each row

    // Case 1: (ε,δ)=(0.1, 0.1) -> Actual: 95, Estimate: 3309296
    // double epsilon = 0.1;
    // double confidence = 0.1;

    // Case 2: (ε,δ)=(0.01, 0.1) -> Actual: 95, Estimate: 3303190
    // double epsilon = 0.01;
    // double confidence = 0.1;

    // Case 3: (ε,δ)=(0.1, 0.01)  -> Actual: 95, Estimate: 1980904
    double epsilon = 0.1;
    double confidence = 0.01;

    int rows = (int) Math.ceil(Math.exp(1) / epsilon);
    int width = (int) Math.ceil(Math.log(1 / confidence));

    // int rows = 100;
    // int width = 100;

    CountMinSketch.init(rows, width);

    // For the partitions we add it to a sketch
    JavaRDD<CountMinSketch> localSketches = songIDs.mapPartitions(partition -> {
      CountMinSketch localSketch = new CountMinSketch();
      while (partition.hasNext())
        localSketch.add(partition.next());
      return Arrays.asList(localSketch).iterator();
    });

    // Combine the sketches
    CountMinSketch totalSketch = localSketches.reduce((sketch1, sketch2) -> {
      return CountMinSketch.merge(sketch1, sketch2);
    });

    System.out.println(totalSketch);

    // Find the actual amount of plays for each song
    JavaPairRDD<Integer, Integer> songIDCount = songIDs.mapToPair(songId -> new Tuple2<Integer, Integer>(songId, 1));
    songIDCount.take(10).forEach(System.out::println);
    JavaPairRDD<Integer, Integer> songIDSum = songIDCount.reduceByKey((a,b) -> a+b);
    songIDSum.take(10).forEach(System.out::println);

    // The ID to estimate
    int songid = 63829;

    // Get the actual value
    List<Integer> counts = songIDSum.lookup(songid);
    int actual = counts.isEmpty() ? 0 : counts.get(0);
    // Get the estimate
    int estimate = totalSketch.getFreq(songid);

    // Print result (actual vs. estimate)
    System.out.println("Actual Count: " + actual);
    System.out.println( "Estimate: " + estimate);

    // Calculate the error
    int error = ((estimate + actual) / actual) *100;
    System.out.println( "Error: " + error);

    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
    spark.stop();
  }
}
