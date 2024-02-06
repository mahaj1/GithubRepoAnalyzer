import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class GitHubDataProducer {

    private static final String TOPIC_NAME = "github-data-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Change to your Kafka bootstrap servers

    public static void main(String[] args) {
        System.out.println("########### GitHub Data Producer started running..........");
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

        String fileName = "/Users/saleh/Desktop/MIU Resources/BDT/GithubRepoAnalyzer/GitHubDataProducer/src/files/organization_list.txt";
        List<String> organizationNames;
        try {
            organizationNames = readOrganizationNamesFromFile(fileName);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("########## GitHub Data Producer is in step 2......");
        System.out.println("================================================");
        System.out.println("########## Found " + organizationNames.size() + " organizations to process ##########");
        System.out.println("================================================");
        try {
            for (String orgName : organizationNames) {
                System.out.println("Processing organization: " + orgName);
                jsonData = gitHubApiClient.fetchGitHubData(orgName); // Implement this method using a GitHub API library
                // Send data to Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, jsonData);
                producer.send(record);
                System.out.println("Processing " + orgName + " repositories is done....   \n\n***** Waiting for 15 seconds to process next org...");
                // Sleep for 15 seconds
                Thread.sleep(15000);

            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Close the producer
        producer.close();
    }


    private static List<String> readOrganizationNamesFromFile(String fileName) throws IOException {
        List<String> organizationNames = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;

            while ((line = reader.readLine()) != null) {
                organizationNames.add(line.trim());
            }
        }

        return organizationNames;
    }


}

