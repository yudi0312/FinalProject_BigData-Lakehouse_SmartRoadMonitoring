"""
Silver Layer ETL - Data Cleaning and Standardization
Membaca dari Bronze Layer, melakukan data cleaning, missing value handling,
geocoding, dan feature standardization, kemudian tulis ke Silver Layer.
"""
import logging
import os
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Add parent directory to path to import config and services
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from config import HDFS_PATHS, POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD, get_date_partition
from silver.geocoding_service import GeocodingService
from silver.standardization import StandardizationService, create_standardization_udfs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverLayer:
    """
    Silver Layer ETL Handler.
    Mengubah data dari Bronze (raw) menjadi Silver (clean dan standardized)
    dengan deduplication, missing value handling, geocoding, dan standardization.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")

    def clean_reports(self, df: DataFrame) -> DataFrame:
        """
        Clean dan transform road reports.
        - Remove duplicates
        - Normalize column names
        - Fix date formats
        - Handle missing values
        - Geocoding
        - Feature standardization
        """
        logger.info("Cleaning reports data...")

        # Remove duplicates berdasarkan report_id dan event_time
        df = df.dropDuplicates(["report_id", "event_time"])

        # Standardize district names
        standardize_district_udf = F.udf(
            StandardizationService.standardize_district_name,
            StringType()
        )
        df = df.withColumn("district", standardize_district_udf(F.col("district")))

        # Standardize damage types
        standardize_damage_udf = F.udf(
            StandardizationService.standardize_damage_type,
            StringType()
        )
        df = df.withColumn("damage_type", standardize_damage_udf(F.col("damage_type")))

        # Handle missing latitude/longitude dengan geocoding
        geocode_udf = F.udf(
            lambda dist: GeocodingService.geocode_by_district(dist),
            "struct<latitude:double,longitude:double>"
        )
        
        geocoded = df.withColumn("geocoded", geocode_udf(F.col("district")))
        df = geocoded.withColumn(
            "latitude",
            F.when(
                F.col("latitude").isNull() | (F.col("latitude") == 0),
                F.col("geocoded.latitude")
            ).otherwise(F.col("latitude"))
        ).withColumn(
            "longitude",
            F.when(
                F.col("longitude").isNull() | (F.col("longitude") == 0),
                F.col("geocoded.longitude")
            ).otherwise(F.col("longitude"))
        ).drop("geocoded")

        # Handle missing severity_score - fill with 0
        df = df.withColumn(
            "severity_score",
            F.when(F.col("severity_score").isNull(), 0).otherwise(F.col("severity_score"))
        )

        # Handle missing confidence - fill with 0.0
        df = df.withColumn(
            "confidence",
            F.when(F.col("confidence").isNull(), 0.0).otherwise(F.col("confidence"))
        )

        # Add processing metadata
        df = df.withColumn("processed_timestamp", F.current_timestamp())
        df = df.withColumn("data_quality_score", F.lit(1.0))

        # Select and reorder columns
        df = df.select(
            F.col("event_id"),
            F.col("event_time"),
            F.col("report_id"),
            F.col("road_name"),
            F.col("district"),
            F.col("village"),
            F.col("latitude").cast(DoubleType()),
            F.col("longitude").cast(DoubleType()),
            F.col("damage_type"),
            F.col("severity_score").cast(IntegerType()),
            F.col("confidence").cast(DoubleType()),
            F.col("status"),
            F.col("ingestion_timestamp"),
            F.col("source"),
            F.col("processed_timestamp"),
            F.col("data_quality_score"),
        )

        return df

    def clean_weather(self, df: DataFrame) -> DataFrame:
        """Clean dan standardize weather data"""
        logger.info("Cleaning weather data...")

        # Remove duplicates
        df = df.dropDuplicates(["event_id", "event_time"])

        # Standardize district
        standardize_district_udf = F.udf(
            StandardizationService.standardize_district_name,
            StringType()
        )
        df = df.withColumn("district", standardize_district_udf(F.col("district")))

        # Standardize weather condition
        standardize_condition_udf = F.udf(
            StandardizationService.standardize_weather_condition,
            StringType()
        )
        df = df.withColumn("condition", standardize_condition_udf(F.col("condition")))

        # Handle missing values
        df = df.withColumn("rainfall_mm", F.when(F.col("rainfall_mm").isNull(), 0.0).otherwise(F.col("rainfall_mm")))
        df = df.withColumn("temperature_c", F.when(F.col("temperature_c").isNull(), F.avg("temperature_c").over(Window.partitionBy())).otherwise(F.col("temperature_c")))
        df = df.withColumn("humidity", F.when(F.col("humidity").isNull(), 0.5).otherwise(F.col("humidity")))

        # Add processing metadata
        df = df.withColumn("processed_timestamp", F.current_timestamp())

        return df

    def clean_traffic(self, df: DataFrame) -> DataFrame:
        """Clean dan standardize traffic data"""
        logger.info("Cleaning traffic data...")

        # Remove duplicates
        df = df.dropDuplicates(["event_id", "event_time"])

        # Standardize district
        standardize_district_udf = F.udf(
            StandardizationService.standardize_district_name,
            StringType()
        )
        df = df.withColumn("district", standardize_district_udf(F.col("district")))

        # Standardize congestion level
        standardize_congestion_udf = F.udf(
            StandardizationService.standardize_congestion_level,
            StringType()
        )
        df = df.withColumn("congestion_level", standardize_congestion_udf(F.col("congestion_level")))

        # Handle missing values
        df = df.withColumn("average_speed_kmh", F.when(F.col("average_speed_kmh").isNull(), 0.0).otherwise(F.col("average_speed_kmh")))
        df = df.withColumn("vehicle_count", F.when(F.col("vehicle_count").isNull(), 0).otherwise(F.col("vehicle_count")))

        # Add processing metadata
        df = df.withColumn("processed_timestamp", F.current_timestamp())

        return df

    def clean_news(self, df: DataFrame) -> DataFrame:
        """Clean dan standardize news data"""
        logger.info("Cleaning news data...")

        # Remove duplicates
        df = df.dropDuplicates(["event_id"])

        # Standardize district
        standardize_district_udf = F.udf(
            StandardizationService.standardize_district_name,
            StringType()
        )
        df = df.withColumn("district", standardize_district_udf(F.col("district")))

        # Handle missing sentiment_score
        df = df.withColumn("sentiment_score", F.when(F.col("sentiment_score").isNull(), 0.0).otherwise(F.col("sentiment_score")))

        # Add processing metadata
        df = df.withColumn("processed_timestamp", F.current_timestamp())

        return df

    def clean_accidents(self, df: DataFrame) -> DataFrame:
        """Clean dan standardize accident data"""
        logger.info("Cleaning accident data...")

        # Remove duplicates
        df = df.dropDuplicates(["accident_id", "event_time"])

        # Standardize district
        standardize_district_udf = F.udf(
            StandardizationService.standardize_district_name,
            StringType()
        )
        df = df.withColumn("district", standardize_district_udf(F.col("district")))

        # Standardize severity
        standardize_severity_udf = F.udf(
            StandardizationService.standardize_severity_level,
            StringType()
        )
        df = df.withColumn("severity", standardize_severity_udf(F.col("severity")))

        # Handle missing coordinates dengan geocoding
        geocode_udf = F.udf(
            lambda dist: GeocodingService.geocode_by_district(dist),
            "struct<latitude:double,longitude:double>"
        )
        
        geocoded = df.withColumn("geocoded", geocode_udf(F.col("district")))
        df = geocoded.withColumn(
            "latitude",
            F.when(F.col("latitude").isNull(), F.col("geocoded.latitude")).otherwise(F.col("latitude"))
        ).withColumn(
            "longitude",
            F.when(F.col("longitude").isNull(), F.col("geocoded.longitude")).otherwise(F.col("longitude"))
        ).drop("geocoded")

        # Handle missing values
        df = df.withColumn("vehicle_count", F.when(F.col("vehicle_count").isNull(), 0).otherwise(F.col("vehicle_count")))
        df = df.withColumn("casualties", F.when(F.col("casualties").isNull(), 0).otherwise(F.col("casualties")))

        # Add processing metadata
        df = df.withColumn("processed_timestamp", F.current_timestamp())

        return df

    def write_parquet_with_partitions(self, df: DataFrame, path: str, partition_cols: list) -> None:
        """Write cleaned data to Parquet with date and region partitions"""
        if df.rdd.isEmpty():
            logger.warning(f"Empty dataframe, skipping write to {path}")
            return

        df.write \
            .mode("append") \
            .partitionBy(*partition_cols) \
            .parquet(path)

        logger.info(f"Written data to {path} with partitions {partition_cols}")


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SRIS Silver Layer") \
        .getOrCreate()

    silver = SilverLayer(spark)

    # Read from Bronze reports and clean
    logger.info("Reading bronze reports...")
    reports_bronze = spark.read.parquet(HDFS_PATHS["bronze_reports"])
    reports_silver = silver.clean_reports(reports_bronze)
    
    # Write to Silver with partitions
    silver.write_parquet_with_partitions(
        reports_silver,
        HDFS_PATHS["silver_reports"],
        ["year", "month", "day", "country", "province", "city"]
    )

    # Read and clean weather
    logger.info("Reading bronze weather...")
    weather_bronze = spark.read.parquet(HDFS_PATHS["bronze_weather"])
    weather_silver = silver.clean_weather(weather_bronze)
    silver.write_parquet_with_partitions(
        weather_silver,
        HDFS_PATHS["silver_weather"],
        ["year", "month", "day"]
    )

    # Read and clean traffic
    logger.info("Reading bronze traffic...")
    traffic_bronze = spark.read.parquet(HDFS_PATHS["bronze_traffic"])
    traffic_silver = silver.clean_traffic(traffic_bronze)
    silver.write_parquet_with_partitions(
        traffic_silver,
        HDFS_PATHS["bronze_traffic"],
        ["year", "month", "day"]
    )

    # Read and clean news
    logger.info("Reading bronze news...")
    news_bronze = spark.read.parquet(HDFS_PATHS["bronze_news"])
    news_silver = silver.clean_news(news_bronze)
    silver.write_parquet_with_partitions(
        news_silver,
        HDFS_PATHS["silver_news"],
        ["year", "month", "day"]
    )

    # Read and clean accidents
    logger.info("Reading bronze accidents...")
    accidents_bronze = spark.read.parquet(HDFS_PATHS["bronze_accidents"])
    accidents_silver = silver.clean_accidents(accidents_bronze)
    silver.write_parquet_with_partitions(
        accidents_silver,
        HDFS_PATHS["silver_accidents"],
        ["year", "month", "day", "country", "province", "city"]
    )

    logger.info("Silver layer ETL completed!")
