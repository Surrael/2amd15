package nl.tue.bdm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.clustering.KMeansModel;

import java.util.List;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.VectorAssembler;

public class Main {
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .appName("2AMD15")
        .master("local[*]")
        .getOrCreate();

    StructType schema = new StructType(new StructField[] {
        new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("song_id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("rating", DataTypes.IntegerType, true, Metadata.empty()) // null is allowed
    });

    Dataset<Row> df = spark.read()
        .option("header", "false")
        .schema(schema)
        .csv("plays.csv");

    // filter out all the lines without ratings
    df = df.filter("rating IS NOT NULL");

    // Assemble features into a single vector (KMeans cannot be trained with a
    // 3-column df) we need a feature vector [id, song, rating] per row
    VectorAssembler assembler = new VectorAssembler()
        .setInputCols(new String[] { "user_id", "song_id", "rating" })
        .setOutputCol("features");

    Dataset<Row> featureDf = assembler.transform(df);

    // Create and train a KMeans model
    KMeans kmeans = new KMeans().setK(100).setSeed(1L);
    KMeansModel model = kmeans.fit(featureDf);

    // Make predictions
    Dataset<Row> predictions = model.transform(featureDf);

    int userid = 41391; // the user we will find recommendation for

    // Register DataFrame as a temporary SQL table
    predictions.createOrReplaceTempView("predictions");

    // Run SQL query to get the user's cluster
    Dataset<Row> userCluster = spark.sql(
        "SELECT DISTINCT prediction FROM predictions WHERE user_id = " + userid);

    // Extract the cluster ID as an integer
    List<Row> clusterList = userCluster.collectAsList();
    if (clusterList.isEmpty()) {
      System.out.println("User " + userid + " not found in dataset.");
      spark.stop();
      return;
    }
    Integer userClusterId = clusterList.get(0).getInt(0);

    // The songs in the user's cluster, order from highest avg rating to lowest
    String songsInUserCluster = "SELECT song_id, AVG(rating) as avg_rating " +
        "FROM predictions " +
        "WHERE prediction = " + userClusterId +
        " GROUP BY song_id " +
        "ORDER BY avg_rating DESC";

    // Exclude the songs they've already listened to to find the best
    // recommendations
    Dataset<Row> userRecommendations = spark.sql(
        "SELECT song_id, avg_rating " +
            "FROM (" + songsInUserCluster + ") " +
            "WHERE song_id NOT IN (SELECT song_id FROM predictions WHERE user_id = " + userid + ")");

    userRecommendations.show();

    spark.stop();
  }

}
