package nl.tue.bdm;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

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

    CountMinSketch totalSketch = localSketches.reduce(CountMinSketch::merge);

    // Estimate song frequencies
    JavaPairRDD<Integer, Integer> estimatedCounts = songIds.distinct()
        .mapToPair(song -> new Tuple2<>(song, totalSketch.getFreq(song)));

    // Find the most frequently played song
    Tuple2<Integer, Integer> mostListenedSong = estimatedCounts
        .reduce((song1, song2) -> song1._2 > song2._2 ? song1 : song2);

    System.out.println("Most listened song is songID " + mostListenedSong._1 +
        " with estimated " + mostListenedSong._2 + " plays.");

    sc.close();
  }

}
