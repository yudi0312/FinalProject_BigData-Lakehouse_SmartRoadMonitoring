import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configurations
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")

# Spark Session initialization
spark = SparkSession.builder \
    .appName("SRIS Cleaning: Bronze to Silver") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Initializing Bronze to Silver Processing Job...")

# Define schemas for safe reading if paths don't exist
schemas = {
    "reports": StructType([
        StructField("report_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("image_path", StringType(), True),
        StructField("severity_score", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("village", StringType(), True),
        StructField("confidence", DoubleType(), True)
    ]),
    "weather": StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("rainfall_mm", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("condition", StringType(), True)
    ]),
    "traffic": StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("average_speed_kmh", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
        StructField("vehicle_count", IntegerType(), True)
    ]),
    "news": StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
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

# Safe reading function
def read_bronze_safely(dataset_name, schema):
    path = f"{HDFS_NAMENODE_URL}/raw/{dataset_name}"
    try:
        print(f"Reading Bronze {dataset_name}...")
        df = spark.read.parquet(path)
        # Verify it has rows and contains columns, else fallback
        if len(df.columns) == 0:
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        return df
    except Exception as e:
        print(f"[WARNING] Bronze {dataset_name} path not found or empty: {str(e)}. Using placeholder schema.")
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# 1. Clean Reports
reports_df = read_bronze_safely("reports", schemas["reports"])
if not reports_df.rdd.isEmpty():
    # Remove nulls in report_id and duplicates
    reports_clean = reports_df \
        .filter(col("report_id").isNotNull()) \
        .dropDuplicates(["report_id"]) \
        .filter(
            (col("latitude").between(-90.0, 90.0)) &
            (col("longitude").between(-180.0, 180.0))
        ) \
        .withColumn("road_name", initcap(trim(col("road_name")))) \
        .withColumn("district", initcap(trim(col("district")))) \
        .withColumn("village", initcap(trim(col("village")))) \
        .withColumn("status", trim(col("status"))) \
        .select(
            col("report_id").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("latitude").cast(DoubleType()),
            col("longitude").cast(DoubleType()),
            col("description").cast(StringType()),
            col("image_path").cast(StringType()),
            col("severity_score").cast(IntegerType()),
            col("status").cast(StringType()),
            col("road_name").cast(StringType()),
            col("district").cast(StringType()),
            col("village").cast(StringType()),
            col("confidence").cast(DoubleType())
        )
else:
    reports_clean = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["reports"])

reports_clean.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/silver/reports")
print(f"Silver Reports written. Count: {reports_clean.count()}")

# 2. Clean Weather
weather_df = read_bronze_safely("weather", schemas["weather"])
if not weather_df.rdd.isEmpty():
    weather_clean = weather_df \
        .filter(col("event_id").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("city", initcap(trim(col("city")))) \
        .withColumn("district", initcap(trim(col("district")))) \
        .withColumn("condition", trim(col("condition"))) \
        .select(
            col("event_id").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("city").cast(StringType()),
            col("district").cast(StringType()),
            col("rainfall_mm").cast(DoubleType()),
            col("temperature_c").cast(DoubleType()),
            col("humidity").cast(DoubleType()),
            col("condition").cast(StringType())
        )
else:
    weather_clean = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["weather"])

weather_clean.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/silver/weather")
print(f"Silver Weather written. Count: {weather_clean.count()}")

# 3. Clean Traffic
traffic_df = read_bronze_safely("traffic", schemas["traffic"])
if not traffic_df.rdd.isEmpty():
    traffic_clean = traffic_df \
        .filter(col("event_id").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("road_name", initcap(trim(col("road_name")))) \
        .withColumn("district", initcap(trim(col("district")))) \
        .withColumn("congestion_level", trim(col("congestion_level"))) \
        .select(
            col("event_id").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("road_name").cast(StringType()),
            col("district").cast(StringType()),
            col("average_speed_kmh").cast(DoubleType()),
            col("congestion_level").cast(StringType()),
            col("vehicle_count").cast(IntegerType())
        )
else:
    traffic_clean = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["traffic"])

traffic_clean.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/silver/traffic")
print(f"Silver Traffic written. Count: {traffic_clean.count()}")

# 4. Clean News
news_df = read_bronze_safely("news", schemas["news"])
if not news_df.rdd.isEmpty():
    news_clean = news_df \
        .filter(col("event_id").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("city", initcap(trim(col("city")))) \
        .withColumn("district", initcap(trim(col("district")))) \
        .withColumn("source", trim(col("source"))) \
        .select(
            col("event_id").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("source").cast(StringType()),
            col("city").cast(StringType()),
            col("district").cast(StringType()),
            col("title").cast(StringType()),
            col("sentiment_score").cast(DoubleType()),
            col("keyword").cast(StringType())
        )
else:
    news_clean = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["news"])

news_clean.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/silver/news")
print(f"Silver News written. Count: {news_clean.count()}")

# 5. Clean Accidents (Placeholder structure)
accidents_df = read_bronze_safely("accidents", schemas["accidents"])
if not accidents_df.rdd.isEmpty():
    accidents_clean = accidents_df \
        .filter(col("event_id").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("road_name", initcap(trim(col("road_name")))) \
        .withColumn("district", initcap(trim(col("district")))) \
        .select(
            col("event_id").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("road_name").cast(StringType()),
            col("district").cast(StringType()),
            col("accident_type").cast(StringType()),
            col("severity").cast(StringType()),
            col("casualties").cast(IntegerType())
        )
else:
    accidents_clean = spark.createDataFrame(spark.sparkContext.emptyRDD(), schemas["accidents"])

accidents_clean.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/silver/accidents")
print(f"Silver Accidents written. Count: {accidents_clean.count()}")

print("Bronze to Silver cleansing completed successfully!")
spark.stop()
