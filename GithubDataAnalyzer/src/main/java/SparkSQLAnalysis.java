import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class SparkSQLAnalysis {

    public static void main(String[] args) {
        // Set up Spark configuration and SparkSession
        SparkConf sparkConf = new SparkConf().setAppName("GitHubDataAnalysis").setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")  // Set the Hive warehouse directory
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        // Specify the Hive database and table name
        String databaseName = "default";
        String tableName = "github_repositories";

        // Load data from the Hive table into a DataFrame
        Dataset<Row> repoDF = spark.table(databaseName + "." + tableName);

        // Analysis 1: Count repositories per programming language
        Dataset<Row> langCountDF = repoDF.groupBy("language").count();
        langCountDF.show();
        langCountDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".lang_repo_count");

        // Analysis 2: Average stars and forks per organization
        Dataset<Row> orgAvgStatsDF = repoDF.groupBy("owner")
                .agg(
                        functions.avg("stargazersCount").as("avgStars"),
                        functions.avg("forks").as("avgForks")
                );
        orgAvgStatsDF.show();
        orgAvgStatsDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".org_avg_stats");

        // Analysis 3: Count repositories created each year
        Dataset<Row> repoCountByYearDF = repoDF.withColumn("year", functions.substring(repoDF.col("createdAt"), 1, 4))
                .groupBy("year").count();
        repoCountByYearDF.show();
        repoCountByYearDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".repo_count_by_year");

        // Analysis 4: Maximum stargazers count per programming language
        Dataset<Row> maxStarsPerLangDF = repoDF.groupBy("language").agg(functions.max("stargazersCount").as("maxStars"));
        maxStarsPerLangDF.show();
        maxStarsPerLangDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".max_stars_per_lang");

        // Analysis 5: Total forks and open issues per year
        Dataset<Row> forksOpenIssuesByYearDF = repoDF.withColumn("year", functions.substring(repoDF.col("createdAt"), 1, 4))
                .groupBy("year")
                .agg(
                        functions.sum("forks").as("totalForks"),
                        functions.sum("openIssues").as("totalOpenIssues")
                );
        forksOpenIssuesByYearDF.show();
        forksOpenIssuesByYearDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".forks_open_issues_by_year");

        // Extracting year, owner, stargazersCount, watchers, forks, and openIssues
        Dataset<Row> repoYearlySummaryDF = repoDF
                .withColumn("year", functions.substring(repoDF.col("createdAt"), 1, 4))
                .groupBy("year", "owner")
                .agg(
                        functions.sum("stargazersCount").alias("totalStars"),
                        functions.sum("watchers").alias("totalWatchers"),
                        functions.sum("forks").alias("totalForks"),
                        functions.sum("openIssues").alias("totalOpenIssues")
                );

        repoYearlySummaryDF.show();
        repoYearlySummaryDF.write().mode(SaveMode.Overwrite).saveAsTable(databaseName + ".repo_yearly_summary");
        // Stop the SparkSession
        spark.stop();
    }
}
