import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class GitHubAPIClient {

    private static final String GITHUB_API_URL = "https://api.github.com/orgs/%s/repos?per_page=100&page=1";
    private static final String ORGANIZATION_NAME = "google"; // Replace with your array of organization names

    public String fetchGitHubData() throws IOException {
        String apiUrl = String.format(GITHUB_API_URL, ORGANIZATION_NAME);

        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTPURLConnection object
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // Set request method
        connection.setRequestMethod("GET");

        // Get response code
        int responseCode = connection.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            // Read the response content
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                StringBuilder content = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    content.append(line);
                    System.out.println(line);
                }

                return content.toString();
            }
        } else {
            throw new IOException("Failed to fetch data from GitHub API. Response Code: " + responseCode);
        }
    }
}
