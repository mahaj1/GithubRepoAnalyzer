
## GitHub Data Processing Project

## Overview

This project processes GitHub repository data using Apache Kafka and Apache Spark. It consists of the following three main parts:

1. **Data Ingestion**: GitHub repository data is ingested into Kafka topics.
2. **Streaming Processing**: Spark Streaming processes the data from Kafka, transforms it, and stores it in Hive tables.
3. **Analytical Processing**: Spark SQL is used to perform analytical queries on the processed data and store the results in Hive tables.

## Part 1: Data Ingestion

### Prerequisites

- Apache Kafka

### Setup and Configuration

1. Start Zookeeper:

    ```bash
    zookeeper-server-start.sh config/zookeeper.properties
    ```

2. Start Kafka:

    ```bash
    kafka-server-start.sh config/server.properties
    ```

3. Create a Kafka topic for GitHub data:

    ```bash
    kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic github-data-topic
    ```

### Usage

1. Push GitHub repository data to the Kafka topic "github-data-topic".

---

## Part 2: Streaming Processing

### Prerequisites

- Apache Spark
- Apache Hive
- Maven (for building the project)

### Setup and Configuration

1. Start Hadoop:

    ```bash
    start-dfs.sh
    start-yarn.sh
    ```

2. Start Hive Metastore:

    ```bash
    hive --service metastore
    ```

3. Build and Run Spark Streaming Application:

    ```bash
    mvn clean package
    spark-submit --class SparkStreamingApp --master local[*] target/your-jar-file.jar
    ```

### Usage

1. Ensure data is being ingested into the Kafka topic.
2. Run the Spark Streaming application to process data from Kafka and store it in Hive tables.

---

## Part 3: Analytical Processing

### Prerequisites

- Apache Hive
- Apache Spark
- Maven (for building the project)

### Setup and Configuration

1. Ensure Hadoop, Hive, Kafka, and Spark services are running.

### Usage

1. Run Spark SQL queries to perform analytical processing on the data stored in Hive tables.



## Data Analysis
### Analysis 1: Repository Count by Language

- **Description:** Counts the number of repositories for each programming language.
- **Steps:** Extracts the "language" field from the GitHub repository data, groups by language, and counts the occurrences.

### Analysis 2: Repository Count by Organization

- **Description:** Counts the number of repositories for each organization.
- **Steps:** Extracts the "owner" field from the GitHub repository data, groups by organization, and counts the occurrences.

### Analysis 3: Repository Count by Year

- **Description:** Counts the number of repositories created each year.
- **Steps:** Extracts the year from the "createdAt" field, groups by year, and counts the occurrences.

### Analysis 4: Yearly Summary for Each Organization

- **Description:** Provides a summary for each organization, including total stars, watchers, forks, and open issues per year.
- **Steps:** Groups data by year and organization, calculates total stars, watchers, forks, and open issues.

### Analysis 5: Top 5 Organizations by Repository Count

- **Description:** Identifies the top five organizations with the highest number of repositories.
- **Steps:** Groups data by organization, counts the number of repositories, and selects the top five organizations.

### Analysis 6: Top 5 Repositories by Stars

- **Description:** Lists the top five repositories with the highest number of stars.
- **Steps:** Orders data by stars in descending order and selects the top five repositories.

These analyses aim to provide insights into repository distribution, organizational contributions, temporal trends, and top-performing entities within the GitHub dataset.