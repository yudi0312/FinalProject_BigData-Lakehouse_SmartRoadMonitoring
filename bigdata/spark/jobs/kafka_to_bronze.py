import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofmonth, current_timestamp, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")

# Spark Session initialization
spark = SparkSession.builder \
    .appName("SRIS Ingestion: Kafka to Bronze") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Initializing Ingestion Job...")

# Helper to create partitioned year/month/day columns
def add_partition_columns(df, time_col="timestamp"):
    return df \
        .withColumn("year", year(col(time_col))) \
        .withColumn("month", month(col(time_col))) \
        .withColumn("day", dayofmonth(col(time_col)))

# Define schemas for each Kafka source
schemas = {
    "reports": StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("report_id", StringType(), True),
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("village", StringType(), True),
        StructField("damage_type", StringType(), True),
        StructField("severity_score", IntegerType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("image_path", StringType(), True)
    ]),
    "weather": StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("rainfall_mm", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("condition", StringType(), True)
    ]),
    "traffic": StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("average_speed_kmh", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
        StructField("vehicle_count", IntegerType(), True)
    ]),
    "news": StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("source", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("title", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("keyword", StringType(), True)
    ]),
    "accidents": StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("accident_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("casualties", IntegerType(), True)
    ])
}

# Function to read a Kafka topic safely with fallback to an empty DataFrame on failure/empty
def read_kafka_safely(topic, schema):
    try:
        print(f"Attempting to read from Kafka topic: {topic}...")
        raw_df = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Check if topic is empty
        if raw_df.rdd.isEmpty():
            print(f"Topic '{topic}' is empty. Returning empty DataFrame placeholder.")
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
            
        # Parse JSON from value column
        parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        return parsed_df
    except Exception as e:
        print(f"[WARNING] Failed to read topic '{topic}' from Kafka: {str(e)}. Returning empty DataFrame placeholder.")
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# 1. Process Reports
reports_raw = read_kafka_safely("road_reports", schemas["reports"])
if reports_raw.rdd.isEmpty():
    # Construct empty reports df with timestamp
    reports_bronze = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["reports"]) \
        .withColumn("timestamp", lit(None).cast(TimestampType()))
else:
    reports_bronze = reports_raw.select(
        col("report_id"),
        coalesce(to_timestamp(col("event_time")), current_timestamp()).alias("timestamp"),
        col("latitude"),
        col("longitude"),
        coalesce(col("description"), lit("Kerusakan jalan dilaporkan oleh YOLO/Crowdsourcing.")).alias("description"),
        coalesce(col("image_path"), lit("")).alias("image_path"),
        col("severity_score"),
        coalesce(col("status"), lit("Pending")).alias("status"),
        col("road_name"),
        col("district"),
        col("village"),
        col("confidence")
    )
reports_partitioned = add_partition_columns(reports_bronze)
reports_partitioned.write.mode("append").partitionBy("year", "month", "day").parquet(f"{HDFS_NAMENODE_URL}/raw/reports")
print("Reports Bronze layer written.")

# 2. Process Weather
weather_raw = read_kafka_safely("weather_data", schemas["weather"])
if weather_raw.rdd.isEmpty():
    weather_bronze = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["weather"]) \
        .withColumn("timestamp", lit(None).cast(TimestampType()))
else:
    weather_bronze = weather_raw.select(
        col("event_id"),
        coalesce(to_timestamp(col("event_time")), current_timestamp()).alias("timestamp"),
        col("city"),
        col("district"),
        col("rainfall_mm"),
        col("temperature_c"),
        col("humidity"),
        col("condition")
    )
weather_partitioned = add_partition_columns(weather_bronze)
weather_partitioned.write.mode("append").partitionBy("year", "month", "day").parquet(f"{HDFS_NAMENODE_URL}/raw/weather")
print("Weather Bronze layer written.")

# 3. Process Traffic
traffic_raw = read_kafka_safely("traffic_data", schemas["traffic"])
if traffic_raw.rdd.isEmpty():
    traffic_bronze = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["traffic"]) \
        .withColumn("timestamp", lit(None).cast(TimestampType()))
else:
    traffic_bronze = traffic_raw.select(
        col("event_id"),
        coalesce(to_timestamp(col("event_time")), current_timestamp()).alias("timestamp"),
        col("road_name"),
        col("district"),
        col("average_speed_kmh"),
        col("congestion_level"),
        col("vehicle_count")
    )
traffic_partitioned = add_partition_columns(traffic_bronze)
traffic_partitioned.write.mode("append").partitionBy("year", "month", "day").parquet(f"{HDFS_NAMENODE_URL}/raw/traffic")
print("Traffic Bronze layer written.")

# 4. Process News
news_raw = read_kafka_safely("news_data", schemas["news"])
if news_raw.rdd.isEmpty():
    news_bronze = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["news"]) \
        .withColumn("timestamp", lit(None).cast(TimestampType()))
else:
    news_bronze = news_raw.select(
        col("event_id"),
        coalesce(to_timestamp(col("event_time")), current_timestamp()).alias("timestamp"),
        col("source"),
        col("city"),
        col("district"),
        col("title"),
        col("sentiment_score"),
        col("keyword")
    )
news_partitioned = add_partition_columns(news_bronze)
news_partitioned.write.mode("append").partitionBy("year", "month", "day").parquet(f"{HDFS_NAMENODE_URL}/raw/news")
print("News Bronze layer written.")

# 5. Process Accidents (Placeholder structure only - No mock data generation)
print("Creating empty placeholder structure for Accidents Bronze layer...")
accidents_empty = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["accidents"])
accidents_partitioned = add_partition_columns(accidents_empty, "timestamp") \
    .withColumn("year", lit(2026).cast(IntegerType())) \
    .withColumn("month", lit(6).cast(IntegerType())) \
    .withColumn("day", lit(18).cast(IntegerType()))
accidents_partitioned.write.mode("append").partitionBy("year", "month", "day").parquet(f"{HDFS_NAMENODE_URL}/raw/accidents")
print("Accidents Bronze layer placeholder written.")

print("Kafka to Bronze Ingestion completed successfully!")
spark.stop()
