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

import java.util.*;

import static scala.Console.print;


public class SparkStreamingAppV1 {

    public static void main(String[] args) throws InterruptedException {
        // Set up Spark configuration and streaming context
        SparkConf sparkConf = new SparkConf().setAppName("GitHubDataProcessing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Set up Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092"); // Change to your Kafka broker
        kafkaParams.put("group.id", "github-data-group"); // Change to your consumer group
        kafkaParams.put("auto.offset.reset", "earliest"); // Set to start from the beginning
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());

        // Set up Kafka topics
        Set<String> topicsSet = new HashSet<>(Arrays.asList("github-data-topic")); // Change to your Kafka topic

        // Create Kafka DStream
        // Update the Kafka DStream creation part
        JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
        );

        /*
        // Extract relevant fields and do processing
        directKafkaStream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, Repository>) record -> {
            String json = record.value();
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                // Parse the JSON array
                JsonNode jsonArray = objectMapper.readTree(json);
                // Extract key-value pairs for each repository in the array
                return (Iterator<Repository>) extractRepoInfo(jsonArray);
            } catch (Exception e) {
                e.printStackTrace();
                return (Iterator<Repository>) Collections.emptyList();
            }
        }).foreachRDD(rdd -> {
            print("Processing new RDD...");
//            print(rdd);


            // Save the final output to a text file
            rdd.map(repository -> repository.getName() + ", " + repository.getId() + ", " + repository.getCreatedAt()
                            + ", " + repository.getUpdatedAt() + ", " + repository.getSize() + ", " + repository.getPushedAt()
                            + ", " + repository.getHtmlUrl() + ", " + repository.getStargazersCount() + ", "
                            + repository.getLanguage() + ", " + repository.getForks() + ", " + repository.getOpenIssues()
                            + ", " + repository.getWatchers())
                    .saveAsTextFile("/Users/saleh/Desktop/MIU Resources/BDT/GithubRepoAnalyzer/GithubDataStreamer/output"); // Change to your desired output directory
        print("\n\nRDD Mapped:\n\n"+rdd.collect());
        });


        */

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
            print("Processing new RDD...");

            // Save the final output to text files (one file per partition)
            rdd.map(repository -> repository.getName() + ", " + repository.getId() + ", " + repository.getCreatedAt()
                            + ", " + repository.getUpdatedAt() + ", " + repository.getSize() + ", " + repository.getPushedAt()
                            + ", " + repository.getHtmlUrl() + ", " + repository.getStargazersCount() + ", "
                            + repository.getLanguage() + ", " + repository.getForks() + ", " + repository.getOpenIssues()
                            + ", " + repository.getWatchers())
                    .saveAsTextFile("/Users/saleh/Desktop/MIU Resources/BDT/GithubRepoAnalyzer/GithubDataStreamer/output");

            print("\n\nRDD Mapped:\n\n" + rdd.collect());
        });



        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Iterator<Repository> extractRepoInfo(JsonNode jsonArray) {
        List<Repository> result = new ArrayList<>();
        for (JsonNode repoNode : jsonArray) {
            try {
                // Convert the JsonNode to a Repository object
                Repository repository = new Repository(repoNode);
                result.add(repository);
            } catch (Exception e) {
                e.printStackTrace();
                // Handle extraction failure for a specific repository
            }
        }

        print("\n\n\nresultss: \n\n\n" + result + "\n\n\n");
        return result.iterator();
    }

}