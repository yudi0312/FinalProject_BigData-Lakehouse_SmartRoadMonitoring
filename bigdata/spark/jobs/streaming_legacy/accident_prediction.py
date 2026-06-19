"""
Accident Prediction Dataset - Gold Layer
Membuat feature dataset untuk prediksi kecelakaan jalan.
Menggabungkan weather data, traffic data, road condition, dan road health index.
"""
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, from_json, year, month, dayofmonth, lit,
    row_number, lag, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://host.docker.internal:5432/sris_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sris")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sris_password")


# Schema untuk berbagai data stream
reports_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("report_id", StringType()),
    StructField("road_name", StringType()),
    StructField("district", StringType()),
    StructField("damage_type", StringType()),
    StructField("severity_score", IntegerType()),
    StructField("confidence", DoubleType()),
    StructField("status", StringType()),
])

weather_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("city", StringType()),
    StructField("district", StringType()),
    StructField("rainfall_mm", DoubleType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("condition", StringType()),
])

traffic_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("road_name", StringType()),
    StructField("district", StringType()),
    StructField("average_speed_kmh", DoubleType()),
    StructField("congestion_level", StringType()),
    StructField("vehicle_count", IntegerType()),
])

accident_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("accident_id", StringType()),
    StructField("road_name", StringType()),
    StructField("district", StringType()),
    StructField("severity", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("casualties", IntegerType()),
])


def write_batch(batch_df, batch_id: int) -> None:
    """Write accident prediction dataset to Parquet and PostgreSQL"""
    if batch_df.rdd.isEmpty():
        logger.warning(f"Empty batch {batch_id}, skipping write")
        return

    # Add partition columns
    enriched = batch_df.withColumn("batch_time", current_timestamp())
    enriched = enriched.withColumn("year", year(col("batch_time"))) \
        .withColumn("month", month(col("batch_time"))) \
        .withColumn("day", dayofmonth(col("batch_time"))) \
        .withColumn("country", lit("ID")) \
        .withColumn("province", lit("JawaTimur")) \
        .withColumn("city", lit("Surabaya"))

    # Write to Parquet with partitions
    enriched.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "country", "province", "city") \
        .parquet(f"{HDFS_NAMENODE_URL}/gold/accident_prediction")

    # Also write to PostgreSQL for analytics
    enriched.write.format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", "accident_prediction") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logger.info(f"Batch {batch_id} written to accident_prediction")


def main():
    spark = SparkSession \
        .builder \
        .appName("SRIS Accident Prediction Dataset") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Starting accident prediction dataset generation...")

    # Read accident data from Kafka
    accidents = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "accident_data")
        .option("startingOffsets", "latest")
        .load()
    )

    accidents_df = accidents \
        .select(from_json(col("value").cast("string"), accident_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp())

    # Read road reports (for road condition/health)
    reports = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "road_reports")
        .option("startingOffsets", "latest")
        .load()
    )

    reports_df = reports \
        .select(from_json(col("value").cast("string"), reports_schema).alias("data")) \
        .select("data.*")

    # Read weather data
    weather = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "weather_data")
        .option("startingOffsets", "latest")
        .load()
    )

    weather_df = weather \
        .select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
        .select("data.*")

    # Read traffic data
    traffic = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "traffic_data")
        .option("startingOffsets", "latest")
        .load()
    )

    traffic_df = traffic \
        .select(from_json(col("value").cast("string"), traffic_schema).alias("data")) \
        .select("data.*")

    # Feature Engineering for accident prediction
    # Aggregate reports by district (road health/condition)
    road_health = reports_df \
        .groupBy("district") \
        .agg(
            count("*").alias("damage_report_count"),
            avg("severity_score").alias("avg_severity"),
            spark_max("severity_score").alias("max_severity"),
        )

    # Aggregate traffic by district
    traffic_features = traffic_df \
        .groupBy("district") \
        .agg(
            avg("average_speed_kmh").alias("avg_speed"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            count(col("congestion_level")).alias("traffic_event_count"),
        )

    # Latest weather by district
    weather_latest = weather_df \
        .groupBy("district") \
        .agg(
            avg("rainfall_mm").alias("rainfall_mm"),
            avg("temperature_c").alias("temperature_c"),
            avg("humidity").alias("humidity"),
            col("condition").alias("weather_condition"),
        )

    # Create accident prediction dataset by joining all features
    accident_features = accidents_df \
        .select(
            col("accident_id"),
            col("district"),
            col("road_name"),
            col("event_time"),
            col("severity").alias("accident_severity"),
            col("vehicle_count").alias("vehicles_involved"),
            col("casualties"),
        ) \
        .join(road_health, "district", "left") \
        .join(traffic_features, "district", "left") \
        .join(weather_latest, "district", "left")

    # Fill missing values
    accident_features = accident_features \
        .withColumn("damage_report_count", 
                   col("damage_report_count").cast(IntegerType())) \
        .fillna({"damage_report_count": 0, 
                "avg_severity": 0.0,
                "max_severity": 0.0,
                "avg_speed": 30.0,
                "avg_vehicle_count": 0.0,
                "traffic_event_count": 0.0,
                "rainfall_mm": 0.0,
                "temperature_c": 25.0,
                "humidity": 0.5,
                "weather_condition": "Clear"})

    # Add derived features
    accident_features = accident_features \
        .withColumn("processed_timestamp", current_timestamp())

    # Start streaming write
    query = (
        accident_features.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{HDFS_NAMENODE_URL}/checkpoints/gold/accident_prediction")
        .start()
    )

    logger.info("Accident prediction dataset streaming started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
