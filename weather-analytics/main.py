"""
SRIS Weather Analytics — Phase 4

Pipeline:
  Open-Meteo API → ETL (rolling features) → PostgreSQL
                                           → Kafka (weather_data)

Default: loop otomatis setiap 2 jam.
"""

import argparse
import time


def main():
    parser = argparse.ArgumentParser(description="SRIS Weather Analytics - Phase 4")
    parser.add_argument("--days", type=int, default=365,
        help="Jumlah hari historis (default: 365)")
    parser.add_argument("--no-loop", action="store_true",
        help="Jalankan sekali saja, tidak loop")
    parser.add_argument("--interval", type=int, default=2,
        help="Interval update dalam jam (default: 2)")
    parser.add_argument("--skip-kafka", action="store_true",
        help="Skip Kafka producer")
    args = parser.parse_args()

    def run_once():
        from etl.fetch_weather import run_etl
        from kafka_producer.producer import produce_weather

        records = run_etl(past_days=args.days)
        if not records:
            print("[weather] Tidak ada data")
            return

        if not args.skip_kafka:
            produce_weather(records)
        else:
            print("[kafka] Skipped")

        print(f"[weather] Pipeline selesai. Total: {len(records)} records")

    if args.no_loop:
        run_once()
    else:
        print(f"[loop] Update cuaca setiap {args.interval} jam. Ctrl+C untuk stop.")
        while True:
            run_once()
            print(f"[loop] Selesai. Tunggu {args.interval} jam...")
            time.sleep(args.interval * 3600)


if __name__ == "__main__":
    main()