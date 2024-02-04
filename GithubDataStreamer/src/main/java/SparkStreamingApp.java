import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.*;

import static scala.Console.print;


public class SparkStreamingApp {
    public static void main(String[] args) throws InterruptedException {
        // Set up Spark configuration and streaming context
        SparkConf sparkConf = new SparkConf().setAppName("GitHubDataProcessing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Set up Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "github-data-group");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());

        // Set up Kafka topics
        Set<String> topicsSet = new HashSet<>(Arrays.asList("github-data-topic"));

        // Create Kafka DStream
        JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
        );

        // Create a SparkSession with Hive support
        SparkSession spark = SparkSession.builder()
                .appName("GitHubDataProcessing")
//                .config("spark.master", "local")
//                .config("spark.local.dir", "/tmp/spark-local")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")  // Set the Hive warehouse directory
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        String tableName = "github_repositories";

        // Extract relevant fields and do processing
        directKafkaStream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, Repository>) record -> {
            String json = record.value();
            ObjectMapper objectMapper = new ObjectMapper();
            List<Repository> repositories = new ArrayList<>();

            try {
                // Parse the JSON array
                JsonNode jsonArray = objectMapper.readTree(json);

                // Extract key-value pairs for each repository in the array
                for (JsonNode repoNode : jsonArray) {
                    Repository repository = new Repository(repoNode);
                    repositories.add(repository);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return repositories.iterator();
        }).foreachRDD(rdd -> {
            if (!rdd.isEmpty()) { // Check if the RDD is not empty
                print("Processing new RDD...");

                // Convert RDD to DataFrame
                Dataset<Row> repoDF = spark.createDataFrame(rdd, Repository.class);

                // Check if the table exists
                if (spark.catalog().tableExists(tableName)) {
                    // If the table exists, append data
                    System.out.println("Existing schema: " + spark.table(tableName).schema().treeString());
                    repoDF.write().mode(SaveMode.Append).saveAsTable(tableName);
                } else {
                    // If the table doesn't exist, create a new table
                    System.out.println("Table does not exist.");
                    repoDF.write().saveAsTable(tableName);
                }

                print("\n\nRDD Mapped:\n\n" + rdd.collect());

            } else {
                print("RDD is empty. No data to process.\n\n");
                print("Existing Data: \n\n");
                spark.sql("select * from "+tableName).show(10);
            }
        });

        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();
    }

}