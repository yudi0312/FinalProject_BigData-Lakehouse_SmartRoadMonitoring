"""
SRIS Phase 2 — News Analytics Scraper (Kafka Producer Streaming)
================================================================
Perbaikan dari versi sebelumnya:
- FIX bug fatal: `news_data` di log.info() tidak pernah didefinisikan -> NameError saat runtime
- NLP extractor dipisah ke modul nlp_extractor.py (lihat file terpisah), dengan:
    * Multi-road detection (1 berita bisa hasilkan > 1 record, 1 per jalan)
    * Severity numerik (0-100) selain label kategorikal -> siap pakai untuk formula RHI/RPS
    * Confidence score per field & per artikel -> bisa filter hasil low-confidence
    * Sentiment dengan deteksi negasi sederhana
- Payload Kafka sekarang 1 message PER JALAN yang terdeteksi (bukan 1 message per artikel),
  supaya konsumen (Spark Structured Streaming) tidak perlu re-parse array di sisi consumer.
- Tambah field artikel asli (article_id) yang sama di semua road-mention dari artikel yang sama,
  untuk traceability/debug.

Install:
    pip install feedparser googlenewsdecoder newspaper3k lxml_html_clean kafka-python
"""

import feedparser
import json
import hashlib
import psycopg2
from kafka import KafkaProducer
from googlenewsdecoder import gnewsdecoder
from newspaper import Article
import time
import logging
import os
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

from nlp_extractor import NLPExtractor, ExtractionResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "news_data")  # sesuai topic yang dibuat kafka-init

# Dedup persisten lintas-restart: tiap URL yang sudah pernah dikirim ke Kafka
# disimpan di tabel ini, supaya artikel yang sama (masih muncul di hasil RSS
# berkali-kali) tidak dikirim ulang tiap siklus 30 menit.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sris:sris_password@localhost:5433/sris_db"
)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=id&gl=ID&ceid=ID:id"

SEARCH_QUERIES = [
    "jalan rusak Surabaya",
    "jalan berlubang Surabaya",
    "kerusakan jalan Surabaya",
    "jalan ambles Surabaya",
    "jalan bergelombang Surabaya",
    "perbaikan jalan rusak Surabaya",
]

DECODE_DELAY  = 1.0
FETCH_DELAY   = 1.0
ARTICLE_TIMEOUT_RETRIES = 2
SCRAPE_INTERVAL_MINUTES = 30

# Google News RSS bisa balikin sampai ~100 entri per query. Dibatasi supaya
# satu siklus scraping tidak makan waktu berjam-jam. Set None untuk ambil semua.
MAX_ARTICLES_PER_QUERY = int(os.getenv("MAX_ARTICLES_PER_QUERY", "15"))

# Confidence minimum supaya record dikirim ke Kafka. Set 0.0 untuk kirim semua
# (termasuk yang low-confidence -> biar di-filter belakangan di Spark/consumer).
MIN_CONFIDENCE_TO_SEND = 0.0


# ─────────────────────────────────────────────
# DATA CLASS — RAW ARTICLE (sebelum NLP)
# ─────────────────────────────────────────────
@dataclass
class RawArticle:
    source:          str
    title:           str
    google_news_url:  str
    article_url:      Optional[str] = None
    published_at:     Optional[datetime] = None
    content:          str = ""
    scraped_at:       datetime = field(default_factory=datetime.now)

    @property
    def article_id(self) -> str:
        """ID stabil berdasarkan URL, dipakai sebagai key Kafka & traceability."""
        return hashlib.sha256(self.google_news_url.encode("utf-8")).hexdigest()[:16]


# ─────────────────────────────────────────────
# DEDUP TRACKER (PostgreSQL) — persisten lintas-restart
# ─────────────────────────────────────────────
class DedupTracker:
    """
    Melacak URL artikel yang sudah pernah dikirim ke Kafka, disimpan di
    PostgreSQL supaya tetap ada walau script di-restart atau masuk siklus
    30-menit berikutnya. Ini MENCEGAH artikel yang sama (yang masih nongol
    di hasil RSS Google News berkali-kali) dikirim ulang terus-menerus.
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = True
            self._init_table()
            log.info("DedupTracker: konek ke PostgreSQL berhasil.")
        except Exception as e:
            log.error(f"DedupTracker: gagal konek ke PostgreSQL: {e}")
            self.conn = None

    def _init_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS news_seen_urls (
                    article_id      VARCHAR(16) PRIMARY KEY,
                    google_news_url TEXT UNIQUE NOT NULL,
                    title           TEXT,
                    first_seen_at   TIMESTAMP DEFAULT NOW()
                );
            """)

    def is_seen(self, article_id: str) -> bool:
        """True kalau article_id ini sudah pernah dikirim ke Kafka sebelumnya."""
        if not self.conn:
            return False  # fail-open: kalau DB down, anggap belum pernah (jangan blok scraping)
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM news_seen_urls WHERE article_id = %s",
                    (article_id,)
                )
                return cur.fetchone() is not None
        except Exception as e:
            log.warning(f"DedupTracker: gagal cek is_seen: {e}")
            return False

    def mark_seen(self, article_id: str, url: str, title: str):
        """Catat bahwa artikel ini sudah dikirim ke Kafka."""
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO news_seen_urls (article_id, google_news_url, title)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (article_id) DO NOTHING
                """, (article_id, url, title))
        except Exception as e:
            log.warning(f"DedupTracker: gagal mark_seen: {e}")

    def close(self):
        if self.conn:
            self.conn.close()


# ─────────────────────────────────────────────
# GOOGLE NEWS RSS FETCHER
# ─────────────────────────────────────────────
class GoogleNewsFetcher:
    def fetch_feed(self, query: str) -> list[RawArticle]:
        url = GOOGLE_NEWS_RSS.format(query=quote(query))
        feed = feedparser.parse(url)

        if feed.bozo and not feed.entries:
            log.warning(f"Gagal parse feed untuk query '{query}': {feed.bozo_exception}")
            return []

        entries = feed.entries
        if MAX_ARTICLES_PER_QUERY:
            entries = entries[:MAX_ARTICLES_PER_QUERY]

        articles = []
        for entry in entries:
            source_name = entry.get("source", {}).get("title", "unknown")
            published = self._parse_date(entry.get("published"))

            articles.append(RawArticle(
                source=source_name,
                title=entry.get("title", "").strip(),
                google_news_url=entry.get("link", ""),
                published_at=published,
            ))
        log.info(f"  RSS '{query}': {len(articles)} entri diambil "
                 f"(dari {len(feed.entries)} total tersedia)")
        return articles

    def _parse_date(self, raw: Optional[str]) -> Optional[datetime]:
        if not raw:
            return None
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(raw)
        except Exception:
            return None

    def decode_url(self, article: RawArticle) -> RawArticle:
        try:
            result = gnewsdecoder(article.google_news_url, interval=1)
            if result.get("status"):
                article.article_url = result["decoded_url"]
            else:
                log.warning(f"Decode gagal: {result.get('message')}")
        except Exception as e:
            log.warning(f"Decode error '{article.title[:30]}...': {e}")
        return article

    def fetch_content(self, article: RawArticle) -> RawArticle:
        target_url = article.article_url or article.google_news_url
        for attempt in range(ARTICLE_TIMEOUT_RETRIES):
            try:
                art = Article(target_url, language="id")
                art.download()
                art.parse()
                article.content = art.text.strip()
                if not article.title and art.title:
                    article.title = art.title.strip()
                break
            except Exception as e:
                log.warning(f"Fetch content gagal (attempt {attempt+1}): {e}")
                time.sleep(1)
        return article


# ─────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────
def _json_default(obj):
    """
    Serializer khusus untuk datetime -> ISO 8601 dengan 'T' separator
    (contoh: 2026-06-19T06:35:00). Format ini predictable untuk Spark
    TimestampType, beda dengan default str(datetime) yang pakai spasi
    dan offset timezone yang bisa bikin parsing Spark gagal/null.
    """
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    return str(obj)


def get_kafka_producer() -> Optional[KafkaProducer]:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        log.info(f"Berhasil konek ke Kafka Broker di {KAFKA_BROKER}")
        return producer
    except Exception as e:
        log.error(f"Gagal konek ke Kafka: {e}")
        return None


def build_messages(article: RawArticle, extraction: ExtractionResult) -> list[dict]:
    """
    Bangun 1 message Kafka PER JALAN yang terdeteksi di artikel.
    Kalau tidak ada jalan terdeteksi sama sekali, kirim 1 message dengan road_name=None
    supaya artikel tetap tercatat (bisa di-review manual / di-reproses NLP versi lanjut nanti).

    Payload memuat 2 set field:
    1. Field GENERIC yang cocok dengan schema Bronze Layer milik tim (event_id,
       event_time, city, district, title, sentiment_score, keyword) -- supaya
       from_json() di Spark bisa langsung parse tanpa schema mismatch.
    2. Field DETAIL hasil NLP lengkap (road_name, damage_type, severity_level,
       severity_score, complaint_count, road_confidence, dst) -- field ekstra
       ini akan di-ignore oleh Spark selama schema belum di-update untuk
       mencakupnya, tapi tetap tersimpan di Kafka untuk dipakai nanti.
    """
    # event_time: pakai published_at kalau ada, fallback ke scraped_at supaya
    # tidak pernah null (Spark schema mendefinisikan ini sebagai TimestampType).
    event_time = article.published_at or article.scraped_at

    base = {
        # ---- field generic, harus cocok skema Bronze Layer tim ----
        "event_id":        article.article_id,
        "event_time":      event_time,
        "city":            "Surabaya",          # scope project ini cuma Surabaya
        "title":           article.title,
        "sentiment_score": extraction.sentiment_score,

        # ---- field tambahan lain yang sudah ada sebelumnya ----
        "source":          article.source,
        "url":             article.article_url or article.google_news_url,
        "published_at":    article.published_at,
        "scraped_at":      article.scraped_at,
        "complaint_count": extraction.complaint_count,
        "sentiment":       extraction.sentiment,
        "extraction_confidence": extraction.extraction_confidence,

        # alias lama dipertahankan untuk backward-compat kode lain yang
        # mungkin masih baca "article_id" (bukan "event_id")
        "article_id":      article.article_id,
    }

    if not extraction.roads:
        return [{**base, "road_name": None, "district": None,
                 "damage_type": None, "severity_level": None,
                 "severity_score": None, "road_confidence": 0.0,
                 "keyword": None}]

    messages = []
    for road in extraction.roads:
        if road.confidence < MIN_CONFIDENCE_TO_SEND:
            continue
        road_name = None if road.road_name == "UNKNOWN" else road.road_name
        messages.append({
            **base,
            "district":         road.district,
            "road_name":        road_name,
            "damage_type":      road.damage_type,
            "severity_level":   road.severity_level,
            "severity_score":   road.severity_score,
            # "keyword" diisi damage_type, sesuai skema generic milik tim
            "keyword":          road.damage_type,
            "road_confidence":  road.confidence,
        })
    return messages


def send_to_kafka(producer: KafkaProducer, messages: list[dict]) -> int:
    if not producer or not messages:
        return 0
    count = 0
    for msg in messages:
        try:
            producer.send(KAFKA_TOPIC, key=msg["article_id"], value=msg)
            count += 1
        except Exception as e:
            log.error(f"Gagal kirim message ke Kafka: {e}")
    producer.flush()
    return count


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def run_scraper_cycle():
    fetcher = GoogleNewsFetcher()
    nlp = NLPExtractor()
    producer = get_kafka_producer()

    dedup = DedupTracker(DATABASE_URL)
    dedup.connect()

    total_sent = 0
    total_skipped_dup = 0
    seen_this_run = set()  # cegah duplikat ANTAR query dalam siklus yang sama

    for query in SEARCH_QUERIES:
        log.info(f"Query: '{query}'")
        raw_articles = fetcher.fetch_feed(query)

        query_sent = 0
        for i, art in enumerate(raw_articles):
            if art.google_news_url in seen_this_run:
                continue
            seen_this_run.add(art.google_news_url)

            # Cek dedup PERSISTEN (lintas-restart, lintas-siklus) sebelum
            # buang waktu decode + fetch + NLP untuk artikel yang sudah
            # pernah diproses & dikirim sebelumnya.
            article_id = art.article_id
            if dedup.is_seen(article_id):
                total_skipped_dup += 1
                continue

            log.info(f"  [{i+1}/{len(raw_articles)}] Memproses: {art.title[:60]}...")

            try:
                art = fetcher.decode_url(art)
                time.sleep(DECODE_DELAY)

                art = fetcher.fetch_content(art)
                time.sleep(FETCH_DELAY)

                extraction = nlp.extract_all(art.title, art.content)
                messages = build_messages(art, extraction)

                # Kirim LANGSUNG per-artikel (true streaming), bukan numpuk
                # dulu sampai 100 artikel selesai. Supaya consumer bisa lihat
                # data masuk real-time, dan kalau scraper di-Ctrl+C di tengah,
                # data yang sudah sempat diproses tidak hilang sia-sia.
                if producer:
                    n = send_to_kafka(producer, messages)
                    query_sent += n
                    total_sent += n
                    for msg in messages:
                        log.info(f"    → terkirim: road={msg['road_name']} "
                                 f"severity={msg['severity_level']} sentiment={msg['sentiment']}")
                    # Tandai sudah dikirim HANYA setelah sukses kirim ke Kafka,
                    # supaya kalau Kafka gagal, artikel ini akan dicoba lagi
                    # di siklus berikutnya (bukan hilang selamanya).
                    dedup.mark_seen(article_id, art.google_news_url, art.title)
                else:
                    for msg in messages:
                        print(f"  road={msg['road_name']} | severity={msg['severity_level']} "
                              f"| sentiment={msg['sentiment']} | conf={msg['road_confidence']}")

            except Exception as e:
                log.error(f"  [ERROR] Gagal memproses '{art.title[:50]}': {e}")

        log.info(f"  Query '{query}' selesai: {query_sent} record terkirim ke Kafka.")

    if producer:
        producer.close()
    dedup.close()
    log.info(f"Siklus selesai. Total {total_sent} record baru dikirim ke Kafka, "
             f"{total_skipped_dup} artikel dilewati (sudah pernah diproses sebelumnya).\n")


if __name__ == "__main__":
    while True:
        log.info("=== Memulai siklus scraping berita ===")
        run_scraper_cycle()

        log.info(f"Menunggu {SCRAPE_INTERVAL_MINUTES} menit untuk siklus berikutnya...")
        time.sleep(SCRAPE_INTERVAL_MINUTES * 60)