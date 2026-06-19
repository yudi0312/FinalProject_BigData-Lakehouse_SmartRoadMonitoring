import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, current_timestamp, when, coalesce, abs as spark_abs
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configurations
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://host.docker.internal:5432/sris_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sris")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sris_password")

# Spark Session initialization
spark = SparkSession.builder \
    .appName("SRIS Metrics: Silver to Gold") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Initializing Silver to Gold Processing Job...")

# Define schemas for safe reading
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
def read_silver_safely(dataset_name, schema):
    path = f"{HDFS_NAMENODE_URL}/silver/{dataset_name}"
    try:
        df = spark.read.parquet(path)
        if len(df.columns) == 0:
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        return df
    except Exception as e:
        print(f"[WARNING] Silver {dataset_name} path not found or empty: {str(e)}. Using placeholder schema.")
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# Write to PostgreSQL function
def write_postgres(df, table_name):
    try:
        print(f"Writing to PostgreSQL table '{table_name}'...")
        df.write.format("jdbc") \
            .option("url", POSTGRES_JDBC_URL) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"PostgreSQL table '{table_name}' written successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to write to PostgreSQL table '{table_name}': {str(e)}")

# Read all silver tables
reports = read_silver_safely("reports", schemas["reports"])
weather = read_silver_safely("weather", schemas["weather"])
traffic = read_silver_safely("traffic", schemas["traffic"])
news = read_silver_safely("news", schemas["news"])
accidents = read_silver_safely("accidents", schemas["accidents"])

# Check if reports data is available, as it is the foundation of all metrics
has_reports = not reports.rdd.isEmpty()

# Define baseline aggregates
if has_reports:
    reports_agg = reports.groupBy("road_name", "district").agg(avg("severity_score").alias("severity"))
else:
    reports_agg = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
        StructField("road_name", StringType()),
        StructField("district", StringType()),
        StructField("severity", DoubleType())
    ]))

weather_agg = weather.groupBy("district").agg(avg("rainfall_mm").alias("rainfall")) if not weather.rdd.isEmpty() else \
              spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
                  StructField("district", StringType()), StructField("rainfall", DoubleType())
              ]))

traffic_agg = traffic.groupBy("road_name", "district").agg((avg("vehicle_count") / 20.0).alias("traffic")) if not traffic.rdd.isEmpty() else \
              spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
                  StructField("road_name", StringType()), StructField("district", StringType()), StructField("traffic", DoubleType())
              ]))

accidents_agg = accidents.groupBy("road_name", "district").agg(count("*").alias("accident_count")) if not accidents.rdd.isEmpty() else \
                spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
                    StructField("road_name", StringType()), StructField("district", StringType()), StructField("accident_count", IntegerType())
                ]))

news_agg = news.groupBy("district").agg(count("*").alias("news_count"), avg("sentiment_score").alias("sentiment")) if not news.rdd.isEmpty() else \
           spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
               StructField("district", StringType()), StructField("news_count", IntegerType()), StructField("sentiment", DoubleType())
           ]))


# =====================================================================
# 1. GOLD: Road Health Index (RHI)
# =====================================================================
print("Processing Gold: Road Health Index...")
# Join reports_agg, weather_agg, traffic_agg
rhi_df = reports_agg \
    .join(weather_agg, on="district", how="left") \
    .join(traffic_agg, on=["road_name", "district"], how="left")

# Fallback values
rhi_df = rhi_df \
    .withColumn("rainfall", coalesce(col("rainfall"), lit(0.0))) \
    .withColumn("traffic", coalesce(col("traffic"), lit(0.0))) \
    .withColumn("road_age_score", lit(0.0))

# Calculate index: RHI = 100 - (0.4*severity) - (0.2*rainfall) - (0.2*traffic) - (0.2*road_age_score)
rhi_calculated = rhi_df.withColumn(
    "road_health_index",
    lit(100.0) - (col("severity") * 0.4) - (col("rainfall") * 0.2) - (col("traffic") * 0.2) - (col("road_age_score") * 0.2)
).withColumn("batch_time", current_timestamp())

# Write Gold to HDFS
rhi_calculated.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/gold/road_health_index")
# Write to PostgreSQL
write_postgres(rhi_calculated, "road_health_index")


# =====================================================================
# 2. GOLD: Priority Score
# =====================================================================
print("Processing Gold: Priority Score...")
if has_reports:
    # 2.1 Calculate complaint_score based on total report count per road
    complaints_count_df = reports.groupBy("road_name").agg(count("*").alias("report_count"))
    
    # Normalization rules: 1 -> 10, 2-4 -> 30, 5-9 -> 60, >=10 -> 100
    complaints_score_df = complaints_count_df.withColumn(
        "complaint_score",
        when(col("report_count") == 1, 10.0)
        .when((col("report_count") >= 2) & (col("report_count") <= 4), 30.0)
        .when((col("report_count") >= 5) & (col("report_count") <= 9), 60.0)
        .when(col("report_count") >= 10, 100.0)
        .otherwise(0.0)
    )

    # 2.2 Join fallbacks for traffic_score, accident_score, news_score
    # traffic_score: avg_vehicle_count / 20.0
    traffic_score_df = traffic_agg.select("road_name", "district", col("traffic").alias("traffic_score"))
    
    # accident_score: count of accidents * 10
    accident_score_df = accidents_agg.select("road_name", "district", (col("accident_count") * 10.0).alias("accident_score"))
    
    # news_score: news_count * 10
    news_score_df = news_agg.select("district", (col("news_count") * 10.0).alias("news_score"))

    # Join back to original reports
    priority_joined = reports.select("report_id", "road_name", "district", "severity_score") \
        .join(complaints_score_df, on="road_name", how="left") \
        .join(traffic_score_df, on=["road_name", "district"], how="left") \
        .join(accident_score_df, on=["road_name", "district"], how="left") \
        .join(news_score_df, on="district", how="left")

    # Apply fallback scores (default 0)
    priority_clean = priority_joined \
        .withColumn("traffic_score", coalesce(col("traffic_score"), lit(0.0))) \
        .withColumn("accident_score", coalesce(col("accident_score"), lit(0.0))) \
        .withColumn("complaint_score", coalesce(col("complaint_score"), lit(0.0))) \
        .withColumn("news_score", coalesce(col("news_score"), lit(0.0)))

    # Formula: priority_score = (0.30*severity) + (0.25*traffic) + (0.20*accident) + (0.15*complaint) + (0.10*news)
    priority_calculated = priority_clean.withColumn(
        "priority_score",
        (col("severity_score") * 0.30) +
        (col("traffic_score") * 0.25) +
        (col("accident_score") * 0.20) +
        (col("complaint_score") * 0.15) +
        (col("news_score") * 0.10)
    ).withColumn("batch_time", current_timestamp()) \
     .select("report_id", "road_name", "district", "severity_score", "traffic_score", "accident_score", "complaint_score", "news_score", "priority_score", "batch_time")
else:
    priority_calculated = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
        StructField("report_id", StringType()), StructField("road_name", StringType()), StructField("district", StringType()),
        StructField("severity_score", IntegerType()), StructField("traffic_score", DoubleType()), StructField("accident_score", DoubleType()),
        StructField("complaint_score", DoubleType()), StructField("news_score", DoubleType()), StructField("priority_score", DoubleType()),
        StructField("batch_time", TimestampType())
    ]))

# Write Gold to HDFS & DB
priority_calculated.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/gold/priority_score")
write_postgres(priority_calculated, "priority_score")


# =====================================================================
# 3. GOLD: Damage Prediction (Rule-Based Baseline)
# =====================================================================
print("Processing Gold: Damage Prediction...")
if has_reports:
    damage_pred = reports.select("report_id", "road_name", "district", "severity_score") \
        .withColumn(
            "predicted_risk_level",
            when(col("severity_score") > 80, "High Risk")
            .when((col("severity_score") >= 50) & (col("severity_score") <= 80), "Medium Risk")
            .otherwise("Low Risk")
        ).withColumn("batch_time", current_timestamp())
else:
    damage_pred = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
        StructField("report_id", StringType()), StructField("road_name", StringType()), StructField("district", StringType()),
        StructField("severity_score", IntegerType()), StructField("predicted_risk_level", StringType()), StructField("batch_time", TimestampType())
    ]))

# Write Gold to HDFS & DB
damage_pred.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/gold/damage_prediction")
write_postgres(damage_pred, "damage_prediction")


# =====================================================================
# 4. GOLD: Accident Prediction
# =====================================================================
print("Processing Gold: Accident Prediction...")
# Join RHI details to obtain rainfall and traffic
accident_pred_df = rhi_calculated.select("road_name", "district", "road_health_index", "rainfall", "traffic")

# Calculate accident probability
accident_pred_df = accident_pred_df.withColumn(
    "prob_raw",
    lit(0.05) + (col("rainfall") * 0.01) + (col("traffic") * 0.0001) + ((lit(100.0) - col("road_health_index")) * 0.005)
)
# Cap at 1.0
accident_pred_calculated = accident_pred_df.withColumn(
    "predicted_accident_probability",
    when(col("prob_raw") > 1.0, 1.0).otherwise(col("prob_raw"))
).withColumn(
    "risk_level",
    when(col("predicted_accident_probability") < 0.3, "LOW")
    .when(col("predicted_accident_probability") < 0.6, "MEDIUM")
    .otherwise("HIGH")
).withColumn("prediction_date", current_timestamp()) \
 .withColumn("batch_time", current_timestamp()) \
 .select("road_name", "district", "predicted_accident_probability", "risk_level", "prediction_date", "batch_time")

# Write Gold to HDFS & DB
accident_pred_calculated.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/gold/accident_prediction")
write_postgres(accident_pred_calculated, "accident_prediction")


# =====================================================================
# 5. GOLD: Hotspot Analysis (CPMK-4 GIS Spatial Analytics Support)
# =====================================================================
print("Processing Gold: Hotspot Analysis...")
if has_reports:
    # Aggregated by road_name
    hotspot_df = reports.groupBy("road_name").agg(
        count("*").alias("report_count"),
        avg("severity_score").alias("avg_severity")
    )
    # Calculate hotspot score: (report_count * 5) + (avg_severity * 0.5)
    # Assign road_id using road_name as temporary fallback identifier (or custom road_id if we have it)
    hotspot_calculated = hotspot_df.withColumn(
        "road_id", col("road_name")
    ).withColumn(
        "hotspot_score",
        (col("report_count") * 5.0) + (col("avg_severity") * 0.5)
    ).withColumn("batch_time", current_timestamp()) \
     .select("road_id", "road_name", "report_count", "avg_severity", "hotspot_score", "batch_time")
else:
    hotspot_calculated = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([
        StructField("road_id", StringType()), StructField("road_name", StringType()), StructField("report_count", IntegerType()),
        StructField("avg_severity", DoubleType()), StructField("hotspot_score", DoubleType()), StructField("batch_time", TimestampType())
    ]))

# Write Gold to HDFS & DB
hotspot_calculated.write.mode("overwrite").parquet(f"{HDFS_NAMENODE_URL}/gold/hotspot_analysis")
write_postgres(hotspot_calculated, "hotspot_analysis")

print("Silver to Gold metric calculation completed successfully!")
spark.stop()
