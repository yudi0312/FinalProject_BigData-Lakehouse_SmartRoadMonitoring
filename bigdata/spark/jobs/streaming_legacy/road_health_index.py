import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, expr, from_json, lit, sum as spark_sum, year, month, dayofmonth
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://host.docker.internal:5432/sris_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sris")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sris_password")


schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("report_id", StringType()),
        StructField("road_name", StringType()),
        StructField("district", StringType()),
        StructField("damage_type", StringType()),
        StructField("severity_score", IntegerType()),
        StructField("confidence", DoubleType()),
        StructField("status", StringType()),
    ]
)


def write_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    enriched = batch_df.withColumn("batch_time", current_timestamp())
    # Add partition columns: year, month, day, country, province, city
    enriched = enriched.withColumn("year", year(col("batch_time"))) \
        .withColumn("month", month(col("batch_time"))) \
        .withColumn("day", dayofmonth(col("batch_time"))) \
        .withColumn("country", lit("ID")) \
        .withColumn("province", lit("JawaTimur")) \
        .withColumn("city", lit("Surabaya"))
    
    # Write to Parquet with partitions (optimized storage)
    enriched.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "country", "province", "city") \
        .parquet(f"{HDFS_NAMENODE_URL}/gold/road_health_index")
    
    # Also write to PostgreSQL for analytics
    enriched.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option("dbtable", "road_health_index").option(
        "user", POSTGRES_USER
    ).option("password", POSTGRES_PASSWORD).option("driver", "org.postgresql.Driver").mode("append").save()


spark = SparkSession.builder.appName("SRIS Road Health Index").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "road_reports")
    .option("startingOffsets", "latest")
    .load()
)

reports = events.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

health_index = (
    reports.groupBy("district")
    .agg(
        count("*").alias("report_count"),
        avg("severity_score").alias("average_severity"),
        spark_sum(expr("case when damage_type = 'D40_Pothole' then 1 else 0 end")).alias("pothole_count"),
        spark_sum(expr("case when damage_type like '%Crack%' then 1 else 0 end")).alias("crack_count"),
    )
    .withColumn("road_health_index", lit(100) - col("average_severity"))
)

query = (
    health_index.writeStream.outputMode("complete")
    .foreachBatch(write_batch)
    .option("checkpointLocation", f"{HDFS_NAMENODE_URL}/checkpoints/gold/road_health_index")
    .start()
)

query.awaitTermination()
