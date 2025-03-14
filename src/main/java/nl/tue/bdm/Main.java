package nl.tue.bdm;

import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
  public static void main(String[] args) throws InterruptedException {
    double epsilion = 0.005;
    double confidence = 0.999999999999;

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("sketchSongID");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> lines = sc.textFile("plays.csv");

    JavaRDD<Integer> songIds = lines.map(line -> Integer.valueOf(line.split(",")[0]));

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

    // Print result (actual vs. estimate)
    int songid = 12157;

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
