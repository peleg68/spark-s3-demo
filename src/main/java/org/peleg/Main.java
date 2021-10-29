package org.peleg;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

@Slf4j
public class Main {
    /**
     * The directory for Hadoop home. Change this to your installation path.
     * It is important that the Hadoop WinUtils version matches the hadoop-common version in pom.xml.
     */
    private static final String HADOOP_HOME_DIR = "d:\\winutil\\";
    private static final String S3_ENDPOINT = "192.168.99.100:9000";
    private static final String S3_ACCESS_KEY = "peleg";
    private static final String S3_SECRET_KEY = "peleg686";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR);

        SparkSession sparkSession = SparkSession.builder()
                .appName("s3-demo")
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .master("local[*]")
                .getOrCreate();

        log.info("READING S3 FILE");

        Dataset<String> macbeth = sparkSession.read().textFile("s3a://example-data/macbeth.txt");

        macbeth.show();

        log.info("CALCULATING WORD COUNTS");

        Dataset<Row> wordCounts = macbeth
                .withColumn("word", explode(split(col("value"), "\\W+")))
                .drop("value")
                .withColumn("count", lit(1))
                .groupBy("word")
                .agg(sum("count").as("count"))
                .orderBy(col("count").desc());

        wordCounts.show();

        log.info("WRITING WORD COUNTS TO S3");

        wordCounts
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .csv("s3a://example-data/macbeth-word-counts");
    }
}
