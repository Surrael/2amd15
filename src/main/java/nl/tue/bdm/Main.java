package nl.tue.bdm;

import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * (eps, conf) = (0.1, 0.9)
 * +----------------+--------+
 * | Metric | Value |
 * +----------------+--------+
 * | Actual Count | 7841 |
 * | Estimate | 9558 |
 * +----------------+--------+
 *
 * (eps, conf) = (0.1, 0.99)
 * +----------------+--------+
 * | Metric | Value |
 * +----------------+--------+
 * | Actual Count | 7841 |
 * | Estimate | 8662 |
 * +----------------+--------+
 * 
 * (eps, conf) = (0.01, 0.9)
 * +----------------+--------+
 * | Metric | Value |
 * +----------------+--------+
 * | Actual Count | 7841 |
 * | Estimate | 9155 |
 * +----------------+--------+
 */

public class Main {
  public static void main(String[] args) throws InterruptedException {
    double epsilion = 0.01;
    double confidence = 0.9;

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("sketchSongID");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> lines = sc.textFile("plays.csv");

    JavaRDD<Integer> songIds = lines.map(line -> Integer.valueOf(line.split(",")[1].trim()));

    // Initalize no. of rows/columns and the hash functions for each row
    CountMinSketch.init(epsilion, confidence);

    JavaRDD<CountMinSketch> localSketches = songIds.mapPartitions(partition -> {
      CountMinSketch localSketch = new CountMinSketch();
      while (partition.hasNext())
        localSketch.add(partition.next());
      return Arrays.asList(localSketch).iterator();
    });

    CountMinSketch totalSketch = localSketches.reduce((sketch1, sketch2) -> {
      return CountMinSketch.merge(sketch1, sketch2);
    });

    // Find the actual amount of plays for each song
    JavaPairRDD<Integer, Integer> songIdTuples = songIds.mapToPair(songId -> new Tuple2<Integer, Integer>(songId, 1));

    System.out.println(totalSketch);

    // Print result (actual vs. estimate)
    int songid = 3031;

    List<Integer> counts = songIdTuples.reduceByKey(Integer::sum).lookup(songid);
    int actual = counts.isEmpty() ? 0 : counts.get(0);
    int estimate = totalSketch.getFreq(songid);

    String header = "+----------------+--------+";
    String rowFormat = "| %-14s | %-6d |%n";

    System.out.println(header);
    System.out.printf("| %-14s | %-6s |%n", "Metric", "Value");
    System.out.println(header);
    System.out.printf(rowFormat, "Actual Count", actual);
    System.out.printf(rowFormat, "Estimate", estimate);
    System.out.println(header);

    sc.close();
  }

}
