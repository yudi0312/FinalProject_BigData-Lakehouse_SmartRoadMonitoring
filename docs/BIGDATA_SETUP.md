# SRIS Big Data Layer Setup

Dokumen ini menambahkan layer Big Data terpisah untuk SRIS tanpa mengubah frontend, backend, API, database existing, dashboard, atau integrasi YOLO.

## Komponen

- Apache Kafka + Zookeeper
- Apache Spark Master + Worker
- Hadoop HDFS Namenode + Datanode
- Producer Python terpisah di `bigdata/kafka/producers`
- Spark job terpisah di `bigdata/spark/jobs`

## Menjalankan Kafka, HDFS, dan Spark

Jalankan stack Big Data:

```bash
docker compose -f docker-compose-bigdata.yml up -d
```

Service yang tersedia:

- Kafka: `localhost:9092`
- HDFS Namenode RPC: `localhost:9000`
- HDFS UI: `http://localhost:9870`
- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`

## Membuat Topic Kafka

Topic otomatis dibuat oleh service `kafka-init`:

- `road_reports`
- `weather_data`
- `traffic_data`
- `news_data`

Jika ingin membuat manual:

```bash
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic road_reports --partitions 3 --replication-factor 1
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic weather_data --partitions 3 --replication-factor 1
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic traffic_data --partitions 3 --replication-factor 1
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic news_data --partitions 3 --replication-factor 1
```

Daftar topic:

```bash
docker exec -it sris_kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## Struktur HDFS Data Lake

Folder HDFS otomatis dibuat oleh service `hdfs-init`:

```text
/raw
/raw/news
/raw/weather
/raw/traffic
/raw/reports

/processed
/processed/news
/processed/weather
/processed/traffic
/processed/reports
```

Cek HDFS:

```bash
docker exec -it sris_hdfs_namenode hdfs dfs -ls /
docker exec -it sris_hdfs_namenode hdfs dfs -ls /processed/reports
```

## Producer Kafka

Install dependency producer di mesin lokal:

```bash
pip install -r bigdata/kafka/producers/requirements.txt
```

Kirim sample event:

```bash
python bigdata/kafka/producers/report_producer.py
python bigdata/kafka/producers/weather_producer.py
python bigdata/kafka/producers/traffic_producer.py
python bigdata/kafka/producers/news_producer.py
```

Producer ini hanya mengirim data ke Kafka dan tidak mengubah API existing.

## PostgreSQL Sink Tables

Tabel Big Data baru, tidak mengubah tabel existing:

- `road_health_index`
- `priority_score`
- `damage_prediction`

Buat tabel:

```bash
psql -U sris -d sris_db -f bigdata/spark/sql/create_bigdata_tables.sql
```

Jika PostgreSQL berjalan dari Docker Compose existing, jalankan dari container PostgreSQL sesuai nama container database di project utama.

## Submit Spark Job

Spark job membutuhkan package Kafka connector dan PostgreSQL JDBC driver.

Road Health Index:

```bash
docker exec -it sris_spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /opt/sris/jobs/road_health_index.py
```

Priority Score:

```bash
docker exec -it sris_spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /opt/sris/jobs/priority_score.py
```

Damage Prediction:

```bash
docker exec -it sris_spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /opt/sris/jobs/damage_prediction.py
```

## Environment Penting

Default Spark sink PostgreSQL:

```text
POSTGRES_JDBC_URL=jdbc:postgresql://host.docker.internal:5432/sris_db
POSTGRES_USER=sris
POSTGRES_PASSWORD=sris_password
```

Jika PostgreSQL berada di network Docker lain, sesuaikan `POSTGRES_JDBC_URL` saat menjalankan job atau ubah environment di `docker-compose-bigdata.yml`.

## Menghentikan Big Data Layer

```bash
docker compose -f docker-compose-bigdata.yml down
```

Hapus volume jika ingin reset Kafka dan HDFS:

```bash
docker compose -f docker-compose-bigdata.yml down -v
```
