"""
Bronze Layer - Raw Data Ingestion
Simpan semua data mentah dari Kafka ke HDFS tanpa transformasi.
Setiap dataset memiliki ingestion timestamp dan metadata source.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType, IntegerType

from config import HDFS_PATHS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, get_date_partition, get_region_partition


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeLayer:
    """
    Bronze Layer Ingestion Handler
    Writes raw data from Kafka to HDFS with ingestion metadata
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")

    def ingest_reports(self) -> DataFrame:
        """
        Ingest raw road reports from Kafka
        Adds ingestion_timestamp and source metadata
        """
        logger.info("Starting reports bronze ingestion...")
        
        # Read from Kafka
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPICS["reports"])
            .option("startingOffsets", "latest")
            .load()
        )

        # Parse JSON and add metadata
        df = (
            df
            .select(F.from_json(F.col("value").cast("string"), self._get_reports_schema()).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("kafka_road_reports"))
            .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
            .withColumn("district_partition", F.lower(F.regexp_replace(F.col("district"), " ", "_")))
        )

        return df

    def ingest_weather(self) -> DataFrame:
        """Ingest raw weather data from Kafka"""
        logger.info("Starting weather bronze ingestion...")
        
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPICS["weather"])
            .option("startingOffsets", "latest")
            .load()
        )

        df = (
            df
            .select(F.from_json(F.col("value").cast("string"), self._get_weather_schema()).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("kafka_weather"))
            .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
            .withColumn("district_partition", F.lower(F.regexp_replace(F.col("district"), " ", "_")))
        )

        return df

    def ingest_traffic(self) -> DataFrame:
        """Ingest raw traffic data from Kafka"""
        logger.info("Starting traffic bronze ingestion...")
        
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPICS["traffic"])
            .option("startingOffsets", "latest")
            .load()
        )

        df = (
            df
            .select(F.from_json(F.col("value").cast("string"), self._get_traffic_schema()).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("kafka_traffic"))
            .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        )

        return df

    def ingest_news(self) -> DataFrame:
        """Ingest raw news data from Kafka"""
        logger.info("Starting news bronze ingestion...")
        
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPICS["news"])
            .option("startingOffsets", "latest")
            .load()
        )

        df = (
            df
            .select(F.from_json(F.col("value").cast("string"), self._get_news_schema()).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
            .withColumn("district_partition", F.lower(F.regexp_replace(F.col("district"), " ", "_")))
        )

        return df

    def ingest_accidents(self) -> DataFrame:
        """Ingest raw accident data from Kafka"""
        logger.info("Starting accidents bronze ingestion...")
        
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPICS["accidents"])
            .option("startingOffsets", "latest")
            .load()
        )

        df = (
            df
            .select(F.from_json(F.col("value").cast("string"), self._get_accidents_schema()).alias("data"))
            .select("data.*")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source", F.lit("kafka_accidents"))
            .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
            .withColumn("district_partition", F.lower(F.regexp_replace(F.col("district"), " ", "_")))
        )

        return df

    def write_parquet_with_partitions(self, df: DataFrame, path: str, partition_cols: list):
        """
        Write DataFrame to Parquet with partitioning by date and region.
        Uses append mode to maintain history.
        """
        def write_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.rdd.isEmpty():
                return
            
            # Write with partitions: year, month, day, country, province, city
            batch_df.write \
                .mode("append") \
                .partitionBy(*partition_cols) \
                .parquet(path)
            
            logger.info(f"Written batch {batch_id} to {path}")

        return write_batch

    def start_streaming(self, df: DataFrame, path: str, checkpoint_path: str, name: str, partition_cols: list = None):
        """Start streaming write to Parquet"""
        if partition_cols is None:
            partition_cols = ["year", "month", "day", "country", "province", "city"]
        
        query = (
            df
            .writeStream
            .outputMode("append")
            .foreachBatch(self.write_parquet_with_partitions(df, path, partition_cols))
            .option("checkpointLocation", checkpoint_path)
            .start()
        )
        
        logger.info(f"Started streaming to {path} with checkpoint {checkpoint_path}")
        return query

    @staticmethod
    def _get_reports_schema() -> StructType:
        """Schema for raw road reports"""
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("report_id", StringType()),
            StructField("road_name", StringType()),
            StructField("district", StringType()),
            StructField("village", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("damage_type", StringType()),
            StructField("severity_score", IntegerType()),
            StructField("confidence", DoubleType()),
            StructField("status", StringType()),
        ])

    @staticmethod
    def _get_weather_schema() -> StructType:
        """Schema for raw weather data"""
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("city", StringType()),
            StructField("district", StringType()),
            StructField("rainfall_mm", DoubleType()),
            StructField("temperature_c", DoubleType()),
            StructField("humidity", DoubleType()),
            StructField("condition", StringType()),
        ])

    @staticmethod
    def _get_traffic_schema() -> StructType:
        """Schema for raw traffic data"""
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("road_name", StringType()),
            StructField("district", StringType()),
            StructField("average_speed_kmh", DoubleType()),
            StructField("congestion_level", StringType()),
            StructField("vehicle_count", IntegerType()),
        ])

    @staticmethod
    def _get_news_schema() -> StructType:
        """Schema for raw news data"""
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("source", StringType()),
            StructField("city", StringType()),
            StructField("district", StringType()),
            StructField("title", StringType()),
            StructField("sentiment_score", DoubleType()),
            StructField("keyword", StringType()),
        ])

    @staticmethod
    def _get_accidents_schema() -> StructType:
        """Schema for raw accident data"""
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("accident_id", StringType()),
            StructField("road_name", StringType()),
            StructField("district", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("severity", StringType()),
            StructField("vehicle_count", IntegerType()),
            StructField("casualties", IntegerType()),
        ])


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SRIS Bronze Layer") \
        .getOrCreate()

    bronze = BronzeLayer(spark)

    # Start ingesting all datasets to bronze layer
    queries = []

    # Reports
    reports_df = bronze.ingest_reports()
    reports_query = bronze.start_streaming(
        reports_df,
        HDFS_PATHS["bronze_reports"],
        f"{HDFS_PATHS['checkpoints_bronze']}/reports",
        "bronze_reports",
        ["year", "month", "day", "country", "province", "city"]
    )
    queries.append(reports_query)

    # Weather
    weather_df = bronze.ingest_weather()
    weather_query = bronze.start_streaming(
        weather_df,
        HDFS_PATHS["bronze_weather"],
        f"{HDFS_PATHS['checkpoints_bronze']}/weather",
        "bronze_weather",
        ["year", "month", "day", "country", "province", "city"]
    )
    queries.append(weather_query)

    # Traffic
    traffic_df = bronze.ingest_traffic()
    traffic_query = bronze.start_streaming(
        traffic_df,
        HDFS_PATHS["bronze_traffic"],
        f"{HDFS_PATHS['checkpoints_bronze']}/traffic",
        "bronze_traffic",
        ["year", "month", "day"]
    )
    queries.append(traffic_query)

    # News
    news_df = bronze.ingest_news()
    news_query = bronze.start_streaming(
        news_df,
        HDFS_PATHS["bronze_news"],
        f"{HDFS_PATHS['checkpoints_bronze']}/news",
        "bronze_news",
        ["year", "month", "day"]
    )
    queries.append(news_query)

    # Accidents
    accidents_df = bronze.ingest_accidents()
    accidents_query = bronze.start_streaming(
        accidents_df,
        HDFS_PATHS["bronze_accidents"],
        f"{HDFS_PATHS['checkpoints_bronze']}/accidents",
        "bronze_accidents",
        ["year", "month", "day", "country", "province", "city"]
    )
    queries.append(accidents_query)

    # Wait for all queries
    spark.streams.awaitAnyTermination()
