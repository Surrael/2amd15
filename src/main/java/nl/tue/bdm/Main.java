package nl.tue.bdm;

import scala.Tuple2;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Main {
  public static void main(String[] args) throws InterruptedException {
    double epsilon = 0.005;
    double confidence = 0.999999999999;

    // Define the Spark configuration
    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("distWordCount");

    // Create the context with a 1-second batch size
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    ssc.checkpoint("checkpointdir");

    // Create a DStream that connects to hostname:port, like localhost:9999
    JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

    // Split each line into song IDs
    JavaDStream<Integer> songIds = lines.map(line -> Integer.valueOf(line.split(",")[1].trim()));

    // Initialize CountMinSketch parameters
    CountMinSketch.init(epsilon, confidence);

    // Convert each song ID into a CountMinSketch update with key 0 (global sketch)
    JavaPairDStream<Integer, CountMinSketch> sketches = songIds.mapToPair(
        songId -> {
          CountMinSketch localSketch = new CountMinSketch();
          localSketch.add(songId);
          return new Tuple2<>(0, localSketch); // Key 0 ensures one global sketch
        });

    // Define update function for merging sketches across batches
    Function2<List<CountMinSketch>, Optional<CountMinSketch>, Optional<CountMinSketch>> updateFunction = (newSketches,
        currentState) -> {
      CountMinSketch updatedSketch = currentState.orElse(new CountMinSketch());
      for (CountMinSketch sketch : newSketches) {
        updatedSketch = CountMinSketch.merge(updatedSketch, sketch);
      }
      return Optional.of(updatedSketch);
    };

    // Maintain stateful CountMinSketch
    JavaPairDStream<Integer, CountMinSketch> statefulSketch = sketches.updateStateByKey(updateFunction);

    // Query and print estimates
    statefulSketch.foreachRDD(rdd -> {
      int songId = 12157;
      CountMinSketch totalSketch = rdd.lookup(0).stream().findFirst().orElse(new CountMinSketch());

      System.out.println(totalSketch);

      // Print estimated frequency
      System.out.println("+----------------+--------+");
      System.out.printf("| %-14s | %-6s |%n", "Metric", "Value");
      System.out.println("+----------------+--------+");
      System.out.printf("| %-14s | %-6d |%n", "Estimate", totalSketch.getFreq(songId));
      System.out.println("+----------------+--------+");
    });

    // Start the Spark Streaming Context
    ssc.start();
    ssc.awaitTermination();
  }
}
