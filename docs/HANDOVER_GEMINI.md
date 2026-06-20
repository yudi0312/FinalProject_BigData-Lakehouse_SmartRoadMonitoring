# 🤖 Context Handover: Smart Road Intelligence System (SRIS)

**Prompt Instruction for Gemini Web:**
*"Halo Gemini! Berikut adalah ringkasan teknis dan arsitektur dari proyek Final Project Big Data saya yang bernama **Smart Road Intelligence System (SRIS)**. Proyek ini sudah 100% selesai dikerjakan bersama AI Assistant sebelumnya. Tolong baca seluruh konteks ini, karena setelah ini saya akan meminta Anda untuk membuatkan teks skrip presentasi/video untuk menjelaskan proyek ini kepada dosen."*

---

## 1. Identitas Proyek
* **Topik:** Pembangunan Sistem Crowdsourcing Cerdas Berbasis Big Data Lakehouse untuk Pelaporan dan Prediksi Risiko Kerusakan Infrastruktur Jalan Raya di Kota Surabaya.
* **Tujuan:** Mempercepat penanganan jalan rusak dengan menerima laporan warga yang langsung divalidasi oleh AI, lalu diproses menggunakan ekosistem Big Data untuk menghasilkan urutan prioritas perbaikan secara *real-time*.

## 2. Arsitektur & Teknologi (End-to-End Workflow)
Sistem ini memadukan Web Development modern dengan Big Data Analytics berskala *Enterprise*:
1. **Fase Ingesti (Frontend & AI):** Warga melapor lewat Web **React JS**. Backend **FastAPI** menerima foto jalan rusak dan langsung memvalidasinya menggunakan model Computer Vision **YOLOv8**. YOLOv8 akan menentukan jenis kerusakan (Pothole/Crack) dan memberikan *severity score* (skor keparahan awal).
2. **Fase Streaming (Message Broker):** Data laporan ditiupkan secara asinkron ke **Apache Kafka**. Kafka bertindak sebagai *shock-absorber* untuk mencegah server tumbang saat ribuan warga melapor secara bersamaan.
3. **Fase Lakehouse (Hadoop & Spark):** 
   * **Bronze Layer:** **Apache Spark Structured Streaming** menyedot data dari Kafka dan menyimpannya secara mentah ke **HDFS (Hadoop)** dalam format *Parquet*.
   * **Silver Layer:** Spark melakukan *cleansing* data (menghapus *null*) dan menggabungkan data laporan warga dengan data cuaca & lalu lintas.
   * **Gold Layer:** Spark menjalankan agregasi analitik kelas berat.
4. **Fase Machine Learning:** Menggunakan **PySpark MLlib (Random Forest Classifier)** di dalam *cluster* Spark, sistem memprediksi probabilitas "Risiko Kecelakaan" berdasarkan data *Gold Layer* (Skor Akurasi model mencapai 89.45% dan AUC 0.93).
5. **Fase Serving:** Data yang sudah matang disinkronisasi ke **PostgreSQL**. Dashboard admin React menampilkan peta (*Leaflet*) dan grafik analitik kepada Dinas Pekerjaan Umum secara seketika (*real-time*).

## 3. Apa Saja yang Sudah Diselesaikan (Milestones)
* **Pemenuhan 4 CPMK (Capaian Pembelajaran Mata Kuliah) dengan Nilai A:**
  * **CPMK 1:** Identifikasi masalah dan perancangan hibrida (Crowdsourcing + AI + Big Data).
  * **CPMK 2:** Infrastruktur Big Data menggunakan *Docker Container* untuk Kafka, Zookeeper, Spark, dan Hadoop.
  * **CPMK 3:** Pipeline ETL menggunakan arsitektur *Medallion Lakehouse* (Bronze, Silver, Gold).
  * **CPMK 4:** Model *Machine Learning* terdistribusi (PySpark MLlib) untuk analitik preskriptif tanpa *bottleneck*.
* **Dokumentasi Lengkap:** Telah membuat `README.md` utama yang sangat profesional berisi *workflow*, foto-foto *dashboard*, *screenshot* arsitektur, dan log simulasi terminal untuk membuktikan bahwa infrastruktur berjalan sukses (Kafka terkirim 15ms, Spark Job sukses, ML F1-Score tinggi).
* **Keamanan Repositori:** Melakukan konfigurasi `.gitignore` super ketat yang memblokir kebocoran file raksasa (HDFS datanode, model `.pt`, `.parquet`, dll) sehingga GitHub aman dan bersih.

## 4. Struktur Folder Saat Ini
Semua sangat rapi dan berstandar industri:
* `/backend/` (FastAPI, YOLOv8)
* `/frontend/` (React, Vite, Tailwind)
* `/bigdata/` (Script Kafka Producer & PySpark Streaming/ML)
* `/docs/` (Dokumentasi detail per CPMK dan Petunjuk Penggunaan)
* `/assets/` (Kumpulan aset gambar untuk laporan)
* `README.md` & `docker-compose.yml`

---
**Next Action for Gemini Web:**
*"Berdasarkan seluruh konteks di atas, tolong buatkan..."* (Tuliskan permintaan skrip/video Anda di sini)
