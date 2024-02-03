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

import java.io.Serializable;
import java.util.*;

import static scala.Console.print;

class Repository implements Serializable {
    private String name;
    private String id;
    private String createdAt;
    private String updatedAt;
    private int size;
    private String pushedAt;
    private String htmlUrl;
    private int stargazersCount;
    private String language;
    private int forks;
    private int openIssues;
    private int watchers;

    // Constructors, getters, and setters

    // Add a constructor that takes a JsonNode and populates the fields
    public Repository(JsonNode repoNode) {
        this.name = repoNode.get("name").asText();
        this.id = repoNode.get("id").asText();
        this.createdAt = repoNode.get("created_at").asText();
        this.updatedAt = repoNode.get("updated_at").asText();
        this.size = repoNode.get("size").asInt();
        this.pushedAt = repoNode.get("pushed_at").asText();
        this.htmlUrl = repoNode.get("html_url").asText();
        this.stargazersCount = repoNode.get("stargazers_count").asInt();
        this.language = repoNode.get("language").asText();
        this.forks = repoNode.get("forks").asInt();
        this.openIssues = repoNode.get("open_issues").asInt();
        this.watchers = repoNode.get("watchers").asInt();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getPushedAt() {
        return pushedAt;
    }

    public void setPushedAt(String pushedAt) {
        this.pushedAt = pushedAt;
    }

    public String getHtmlUrl() {
        return htmlUrl;
    }

    public void setHtmlUrl(String htmlUrl) {
        this.htmlUrl = htmlUrl;
    }

    public int getStargazersCount() {
        return stargazersCount;
    }

    public void setStargazersCount(int stargazersCount) {
        this.stargazersCount = stargazersCount;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public int getForks() {
        return forks;
    }

    public void setForks(int forks) {
        this.forks = forks;
    }

    public int getOpenIssues() {
        return openIssues;
    }

    public void setOpenIssues(int openIssues) {
        this.openIssues = openIssues;
    }

    public int getWatchers() {
        return watchers;
    }

    public void setWatchers(int watchers) {
        this.watchers = watchers;
    }
    // Additional methods for cleaning or processing data


    @Override
    public String toString() {
        return "Repository{" +
                "name='" + name + '\'' +
                ", id='" + id + '\'' +
                ", createdAt='" + createdAt + '\'' +
                ", updatedAt='" + updatedAt + '\'' +
                ", size=" + size +
                ", pushedAt='" + pushedAt + '\'' +
                ", htmlUrl='" + htmlUrl + '\'' +
                ", stargazersCount=" + stargazersCount +
                ", language='" + language + '\'' +
                ", forks=" + forks +
                ", openIssues=" + openIssues +
                ", watchers=" + watchers +
                '}';
    }
}
public class SparkStreamingApp {

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