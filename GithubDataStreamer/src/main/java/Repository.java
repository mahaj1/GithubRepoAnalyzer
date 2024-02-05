import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.net.URL;

public class Repository implements Serializable {
    private String owner;
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

        System.out.println("\n======================= forks : " + repoNode.get("forks") + " =======================\n");

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

        // Extract owner from htmlUrl
        try {
            URL url = new URL(this.htmlUrl);
            String[] pathSegments = url.getPath().split("/");
            if (pathSegments.length >= 2) {
                this.owner = pathSegments[1];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getOwner() {
        return owner;
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
                "owner='" + owner + '\'' +
                ", name='" + name + '\'' +
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
