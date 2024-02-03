import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class GitHubDataProducer {

    private static final String TOPIC_NAME = "github-data-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Change to your Kafka bootstrap servers

    public static void main(String[] args) {
        System.out.println("GitHub Data Producer started running...");
        // Configure Kafka producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Fetch data from GitHub API
        GitHubAPIClient gitHubApiClient = new GitHubAPIClient();
        String jsonData;
        System.out.println("GitHub Data Producer is in step 2...");
        try {
            jsonData = gitHubApiClient.fetchGitHubData(); // Implement this method using a GitHub API library
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Send data to Kafka topic
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, jsonData);
        producer.send(record);
        System.out.println("GitHub Data Producer is done!...");

        // Close the producer
        producer.close();
    }
}

