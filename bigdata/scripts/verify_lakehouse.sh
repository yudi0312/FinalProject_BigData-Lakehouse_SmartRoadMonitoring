#!/bin/bash

# ================================================
# SRIS Lakehouse Verification Script
# Verifies Bronze, Silver, and Gold layer setup
# ================================================

set -e

echo "=========================================="
echo "SRIS Lakehouse Verification"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counter
CHECKS_PASSED=0
CHECKS_FAILED=0

# Helper function
check_status() {
    local name=$1
    local status=$2
    
    if [ $status -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $name"
        ((CHECKS_PASSED++))
    else
        echo -e "${RED}✗${NC} $name"
        ((CHECKS_FAILED++))
    fi
}

echo "========== 1. DOCKER CONTAINERS =========="
echo ""

# Check if containers are running
echo "Checking Docker containers..."
docker ps --filter "name=sris" --format "table {{.Names}}\t{{.Status}}" || true
echo ""

# Check Kafka
docker exec -it sris_kafka kafka-brokers --bootstrap-server kafka:29092 &>/dev/null
check_status "Kafka broker accessible" $?

# Check HDFS
docker exec -it sris_hdfs_namenode hdfs dfsadmin -safemode get &>/dev/null
check_status "HDFS NameNode accessible" $?

# Check Spark Master
docker exec -it sris_spark_master spark-submit --version &>/dev/null
check_status "Spark Master accessible" $?

# Check PostgreSQL
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1" &>/dev/null
check_status "PostgreSQL accessible" $?

echo ""
echo "========== 2. KAFKA TOPICS =========="
echo ""

echo "Checking Kafka topics..."
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q "road_reports"
check_status "Topic: road_reports" $?

docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q "weather_data"
check_status "Topic: weather_data" $?

docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q "traffic_data"
check_status "Topic: traffic_data" $?

docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q "news_data"
check_status "Topic: news_data" $?

docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q "accident_data"
check_status "Topic: accident_data" $?

echo ""
echo "========== 3. HDFS DIRECTORIES =========="
echo ""

echo "Checking HDFS directory structure..."

# Bronze directories
docker exec -it sris_hdfs_namenode hdfs dfs -test -d /bronze/reports &>/dev/null
check_status "HDFS /bronze/reports exists" $?

docker exec -it sris_hdfs_namenode hdfs dfs -test -d /bronze/weather &>/dev/null
check_status "HDFS /bronze/weather exists" $?

docker exec -it sris_hdfs_namenode hdfs dfs -test -d /bronze/traffic &>/dev/null
check_status "HDFS /bronze/traffic exists" $?

# Silver directories
docker exec -it sris_hdfs_namenode hdfs dfs -test -d /silver/reports &>/dev/null
check_status "HDFS /silver/reports exists" $?

# Gold directories
docker exec -it sris_hdfs_namenode hdfs dfs -test -d /gold/road_health_index &>/dev/null
check_status "HDFS /gold/road_health_index exists" $?

docker exec -it sris_hdfs_namenode hdfs dfs -test -d /gold/priority_score &>/dev/null
check_status "HDFS /gold/priority_score exists" $?

docker exec -it sris_hdfs_namenode hdfs dfs -test -d /gold/accident_prediction &>/dev/null
check_status "HDFS /gold/accident_prediction exists" $?

# Checkpoint directories
docker exec -it sris_hdfs_namenode hdfs dfs -test -d /checkpoints/bronze &>/dev/null
check_status "HDFS /checkpoints/bronze exists" $?

echo ""
echo "========== 4. POSTGRESQL TABLES =========="
echo ""

echo "Checking PostgreSQL tables..."

# Silver tables
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='silver_reports'" 2>/dev/null | grep -q 1
check_status "Table: silver_reports" $?

docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='silver_weather'" 2>/dev/null | grep -q 1
check_status "Table: silver_weather" $?

docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='silver_accidents'" 2>/dev/null | grep -q 1
check_status "Table: silver_accidents" $?

# Gold tables
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='road_health_index'" 2>/dev/null | grep -q 1
check_status "Table: road_health_index" $?

docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='priority_score'" 2>/dev/null | grep -q 1
check_status "Table: priority_score" $?

docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='damage_prediction'" 2>/dev/null | grep -q 1
check_status "Table: damage_prediction" $?

docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='accident_prediction'" 2>/dev/null | grep -q 1
check_status "Table: accident_prediction" $?

# Metadata tables
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT 1 FROM information_schema.tables WHERE table_name='data_quality_metrics'" 2>/dev/null | grep -q 1
check_status "Table: data_quality_metrics" $?

echo ""
echo "========== 5. PYTHON DEPENDENCIES =========="
echo ""

echo "Checking Python modules..."

# Check if config.py exists and is valid
python3 -c "import sys; sys.path.insert(0, '.'); from bigdata.config import HDFS_PATHS" 2>/dev/null
check_status "bigdata.config module" $?

# Check geocoding service
python3 -c "import sys; sys.path.insert(0, '.'); from bigdata.silver.geocoding_service import GeocodingService" 2>/dev/null
check_status "geocoding_service module" $?

# Check standardization service
python3 -c "import sys; sys.path.insert(0, '.'); from bigdata.silver.standardization import StandardizationService" 2>/dev/null
check_status "standardization module" $?

echo ""
echo "========== 6. DATA QUALITY CHECKS =========="
echo ""

echo "Checking data quality..."

# Check if there's data in PostgreSQL tables
REPORT_COUNT=$(docker exec -it sris_postgres psql -U sris -d sris_db -t -c "SELECT COUNT(*) FROM reports" 2>/dev/null | tr -d ' ')
if [ "$REPORT_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Reports table has data ($REPORT_COUNT records)"
    ((CHECKS_PASSED++))
else
    echo -e "${YELLOW}!${NC} Reports table is empty (normal if no data ingested yet)"
fi

echo ""
echo "========== SUMMARY =========="
echo ""
echo -e "${GREEN}Passed: $CHECKS_PASSED${NC}"
echo -e "${RED}Failed: $CHECKS_FAILED${NC}"
echo ""

if [ $CHECKS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All checks passed! Lakehouse is ready.${NC}"
    exit 0
else
    echo -e "${YELLOW}Some checks failed. Please review above.${NC}"
    exit 1
fi
