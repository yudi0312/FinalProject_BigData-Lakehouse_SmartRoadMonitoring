import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, round as spark_round
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
    enriched.write.mode("append").parquet(f"{HDFS_NAMENODE_URL}/processed/reports/priority_score")
    enriched.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option("dbtable", "priority_score").option(
        "user", POSTGRES_USER
    ).option("password", POSTGRES_PASSWORD).option("driver", "org.postgresql.Driver").mode("append").save()


spark = SparkSession.builder.appName("SRIS Priority Score").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "road_reports")
    .option("startingOffsets", "latest")
    .load()
)

reports = events.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

priority = reports.select(
    "report_id",
    "road_name",
    "district",
    "damage_type",
    "severity_score",
    "confidence",
    spark_round((col("severity_score") * 0.75) + (col("confidence") * 100 * 0.25), 2).alias("priority_score"),
)

query = (
    priority.writeStream.outputMode("append")
    .foreachBatch(write_batch)
    .option("checkpointLocation", f"{HDFS_NAMENODE_URL}/processed/reports/checkpoints/priority_score")
    .start()
)

query.awaitTermination()
