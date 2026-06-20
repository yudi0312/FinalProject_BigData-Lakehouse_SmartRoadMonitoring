"""
Configuration for Smart Road Monitoring Big Data Lakehouse.
Centralized settings for Bronze, Silver, and Gold layers.
"""
import os
from datetime import datetime

# ============ KAFKA CONFIGURATION ============
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

KAFKA_TOPICS = {
    "reports": "road_reports",
    "weather": "weather_data",
    "traffic": "traffic_data",
    "news": "news_data",
    "accidents": "accident_data",
}

# ============ HDFS CONFIGURATION ============
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:9000")

# HDFS Paths for Lakehouse Layers
HDFS_PATHS = {
    # Bronze Layer - Raw data
    "bronze_reports": f"{HDFS_NAMENODE_URL}/bronze/reports",
    "bronze_weather": f"{HDFS_NAMENODE_URL}/bronze/weather",
    "bronze_traffic": f"{HDFS_NAMENODE_URL}/bronze/traffic",
    "bronze_news": f"{HDFS_NAMENODE_URL}/bronze/news",
    "bronze_accidents": f"{HDFS_NAMENODE_URL}/bronze/accidents",
    
    # Silver Layer - Cleaned and standardized data
    "silver_reports": f"{HDFS_NAMENODE_URL}/silver/reports",
    "silver_weather": f"{HDFS_NAMENODE_URL}/silver/weather",
    "silver_traffic": f"{HDFS_NAMENODE_URL}/silver/traffic",
    "silver_news": f"{HDFS_NAMENODE_URL}/silver/news",
    "silver_accidents": f"{HDFS_NAMENODE_URL}/silver/accidents",
    
    # Gold Layer - Analytical datasets
    "gold_road_health": f"{HDFS_NAMENODE_URL}/gold/road_health_index",
    "gold_priority": f"{HDFS_NAMENODE_URL}/gold/priority_score",
    "gold_damage_prediction": f"{HDFS_NAMENODE_URL}/gold/damage_prediction",
    "gold_accident_prediction": f"{HDFS_NAMENODE_URL}/gold/accident_prediction",
    
    # Checkpoints
    "checkpoints_bronze": f"{HDFS_NAMENODE_URL}/checkpoints/bronze",
    "checkpoints_silver": f"{HDFS_NAMENODE_URL}/checkpoints/silver",
    "checkpoints_gold": f"{HDFS_NAMENODE_URL}/checkpoints/gold",
}

# ============ POSTGRESQL CONFIGURATION ============
POSTGRES_JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://host.docker.internal:5432/sris_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sris")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sris_password")

POSTGRES_TABLES = {
    "reports": "reports",
    "road_health_index": "road_health_index",
    "priority_score": "priority_score",
    "damage_prediction": "damage_prediction",
    "accident_prediction": "accident_prediction",
    "silver_reports": "silver_reports",
    "silver_weather": "silver_weather",
    "silver_traffic": "silver_traffic",
    "silver_news": "silver_news",
    "silver_accidents": "silver_accidents",
}

# ============ DATA SCHEMAS ============
# Fields and types for each dataset

BRONZE_SCHEMAS = {
    "reports": [
        "event_id", "event_time", "report_id", "road_name", "district",
        "village", "latitude", "longitude", "damage_type", "severity_score",
        "confidence", "status", "ingestion_timestamp", "source"
    ],
    "weather": [
        "event_id", "event_time", "city", "district", "rainfall_mm",
        "temperature_c", "humidity", "condition", "ingestion_timestamp", "source"
    ],
    "traffic": [
        "event_id", "event_time", "road_name", "district", "average_speed_kmh",
        "congestion_level", "vehicle_count", "ingestion_timestamp", "source"
    ],
    "news": [
        "event_id", "event_time", "source", "city", "district",
        "title", "sentiment_score", "keyword", "ingestion_timestamp"
    ],
    "accidents": [
        "event_id", "event_time", "accident_id", "road_name", "district",
        "latitude", "longitude", "severity", "vehicle_count", "casualties",
        "ingestion_timestamp", "source"
    ],
}

# ============ STANDARDIZATION MAPPINGS ============

# District/Region standardization for Surabaya
DISTRICT_MAPPING = {
    "wonocolo": "Wonocolo",
    "wonokromo": "Wonokromo",
    "tegalsari": "Tegalsari",
    "simokerto": "Simokerto",
    "jambangan": "Jambangan",
    "tandes": "Tandes",
    "rungkut": "Rungkut",
    "gunung_anyar": "Gunung Anyar",
    "sukolilo": "Sukolilo",
    "bulak": "Bulak",
    "krembangan": "Krembangan",
    "sampang": "Sampang",
    "pabean_cantian": "Pabean Cantian",
    "semampir": "Semampir",
    "genteng": "Genteng",
    "usaha_jaya": "Usaha Jaya",
    "karang_pilang": "Karang Pilang",
    "dukuh": "Dukuh",
    "mulyorejo": "Mulyorejo",
    "gubeng": "Gubeng",
    "rungkut": "Rungkut",
    "benowo": "Benowo",
    "pakal": "Pakal",
    "lakarsantri": "Lakarsantri",
    "wiyung": "Wiyung",
    "asemrowo": "Asemrowo",
    "sukomanunggal": "Sukomanunggal",
}

# Damage type standardization
DAMAGE_TYPE_MAPPING = {
    "d00_longitudinal_crack": "Longitudinal Crack",
    "d10_transverse_crack": "Transverse Crack",
    "d20_alligator_crack": "Alligator Crack",
    "d40_pothole": "Pothole",
    "d50_other_damage": "Other Damage",
    "unknown": "Unknown",
}

# Weather condition standardization
WEATHER_CONDITION_MAPPING = {
    "rain": "Rainy",
    "sunny": "Sunny",
    "cloudy": "Cloudy",
    "storm": "Stormy",
    "clear": "Clear",
    "overcast": "Overcast",
}

# Severity level mapping
SEVERITY_LEVEL_MAPPING = {
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "critical": "Critical",
}

# Congestion level standardization
CONGESTION_LEVEL_MAPPING = {
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "severe": "Severe",
}

# ============ PARTITIONING CONFIGURATION ============

# Partition format: year=YYYY/month=MM/day=DD/country=XX/province=XX/city=XX
def get_date_partition(timestamp: str = None) -> str:
    """Generate date partition path from timestamp."""
    if timestamp is None:
        dt = datetime.utcnow()
    else:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

def get_region_partition(country: str = "ID", province: str = "JawaTimur", city: str = "Surabaya") -> str:
    """Generate region partition path."""
    return f"country={country}/province={province}/city={city}"

# ============ SPARK CONFIGURATION ============

SPARK_CONFIG = {
    "spark.app.name": "SRIS Lakehouse ETL",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.streaming.kafka.maxRatePerPartition": "10000",
}

# ============ GEOCODING CONFIGURATION ============

# Default coordinates for Surabaya (fallback)
SURABAYA_DEFAULT_COORDS = {
    "latitude": -7.2575,
    "longitude": 112.7521,
}

# Sample district coordinates (for basic geocoding)
DISTRICT_COORDINATES = {
    "Wonocolo": (-7.335, 112.734),
    "Wonokromo": (-7.315, 112.725),
    "Tegalsari": (-7.265, 112.745),
    "Simokerto": (-7.255, 112.750),
    "Jambangan": (-7.315, 112.800),
    "Tandes": (-7.210, 112.720),
    "Rungkut": (-7.300, 112.800),
    "Gunung Anyar": (-7.280, 112.820),
    "Sukolilo": (-7.245, 112.815),
    "Bulak": (-7.210, 112.745),
    "Krembangan": (-7.230, 112.700),
    "Sampang": (-7.210, 112.760),
    "Pabean Cantian": (-7.225, 112.705),
    "Semampir": (-7.245, 112.710),
    "Genteng": (-7.270, 112.720),
    "Usaha Jaya": (-7.210, 112.650),
    "Karang Pilang": (-7.340, 112.750),
    "Dukuh": (-7.310, 112.760),
    "Mulyorejo": (-7.290, 112.790),
    "Gubeng": (-7.315, 112.795),
    "Benowo": (-7.235, 112.650),
    "Pakal": (-7.210, 112.620),
    "Lakarsantri": (-7.240, 112.680),
    "Wiyung": (-7.300, 112.690),
    "Asemrowo": (-7.340, 112.710),
    "Sukomanunggal": (-7.270, 112.750),
}

# Default missing value strategies
MISSING_VALUE_STRATEGY = {
    "reports": {
        "latitude": "fill_with_district_center",
        "longitude": "fill_with_district_center",
        "severity_score": "fill_with_0",
        "confidence": "fill_with_0.0",
    },
    "weather": {
        "rainfall_mm": "fill_with_0",
        "temperature_c": "fill_with_null",
        "humidity": "fill_with_null",
        "condition": "fill_with_unknown",
    },
    "traffic": {
        "average_speed_kmh": "fill_with_0",
        "vehicle_count": "fill_with_0",
        "congestion_level": "fill_with_low",
    },
}

# ============ DATA QUALITY RULES ============

DATA_QUALITY_RULES = {
    "reports": {
        "latitude": {"min": -7.5, "max": -7.0},
        "longitude": {"min": 112.5, "max": 113.0},
        "severity_score": {"min": 0, "max": 100},
        "confidence": {"min": 0.0, "max": 1.0},
    },
    "weather": {
        "temperature_c": {"min": 15, "max": 45},
        "humidity": {"min": 0.0, "max": 1.0},
        "rainfall_mm": {"min": 0, "max": 500},
    },
    "traffic": {
        "average_speed_kmh": {"min": 0, "max": 120},
        "vehicle_count": {"min": 0, "max": 10000},
    },
}
