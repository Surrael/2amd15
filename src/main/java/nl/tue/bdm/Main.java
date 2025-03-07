package nl.tue.bdm;

import java.io.IOException;

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

    Dataset<Row> df = spark.read().csv("plays.csv");
    df.show(10);

    JavaRDD<String> lines = spark.read().textFile("plays.csv").javaRDD();
    lines.take(10).forEach(System.out::println);

    System.out.println("Press ENTER to close...");
    try {
      System.in.read();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    spark.stop();
  }
}
