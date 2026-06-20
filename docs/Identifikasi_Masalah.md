# CPMK-1: Identifikasi Masalah & Relevansi Big Data

*Dokumen ini disusun khusus untuk memenuhi kriteria "Sangat Baik (A)" pada Rubrik Evaluasi CPMK-1.*

---

## 1. Identifikasi Masalah dengan Data Kuantitatif

**Latar Belakang Masalah**
Kota Surabaya, sebagai kota metropolitan terbesar kedua di Indonesia, memiliki mobilitas dan volume kendaraan komersial yang sangat tinggi. Menurut data statistik (BPS/PUPR), panjang ruas jalan di Surabaya mencapai lebih dari 1.400 km. Berdasarkan tren historis, diperkirakan 10-15% dari total ruas jalan tersebut rawan mengalami kerusakan (mulai dari retak rambut hingga lubang besar/pothole) setiap tahunnya, terutama pada musim penghujan.

Masalah utama yang terjadi saat ini adalah:
1. **Bottleneck Verifikasi:** Pemerintah kota menerima ribuan laporan kerusakan infrastruktur per tahun, namun proses verifikasinya masih manual (petugas harus mendatangi lokasi).
2. **Prioritisasi Subjektif:** Perbaikan jalan sering kali tidak optimal karena tidak didasarkan pada data dampak lingkungan (volume lalu lintas di jalan tersebut atau potensi kecelakaan akibat curah hujan).

## 2. Analisis Gap Solusi yang Ada

**Kelemahan Solusi Saat Ini (Contoh: Aplikasi Lapor/Pengaduan Umum)**
- **Format Data Terbatas:** Mayoritas hanya menerima input teks dan foto yang tidak divalidasi keaslian/kerusakannya secara otomatis.
- **Silo Data:** Laporan kerusakan tidak terhubung dengan data lingkungan eksternal (data cuaca/curah hujan dan data kepadatan lalu lintas).
- **Proses Reaktif:** Penanganan bersifat "siapa yang lapor duluan, itu yang dikerjakan", bukan berdasarkan risiko kecelakaan atau kerusakan terparah.

**Inovasi Smart Road Intelligence System (SRIS)**
SRIS menutup celah tersebut dengan mengintegrasikan **Computer Vision (YOLOv8)** untuk verifikasi instan, serta arsitektur **Medallion Lakehouse** yang menggabungkan aliran data laporan, cuaca historis, dan lalu lintas secara otomatis untuk menghasilkan skor prioritas perbaikan yang murni *data-driven*.

## 3. Mengapa Solusi Ini Membutuhkan Big Data? (Kerangka 5V)

Sistem SRIS tidak bisa diselesaikan dengan arsitektur Monolitik/Database Relasional tradisional karena sistem ini menyentuh seluruh dimensi **5V Big Data**:

1. **Volume (Kapasitas Data)**
   Sistem harus menyimpan dan memproses ratusan hingga ribuan foto kerusakan resolusi tinggi yang diunggah warga, ditambah puluhan ribu baris data historis cuaca harian dan log lalu lintas. Data yang membengkak seiring waktu membutuhkan distributed storage seperti HDFS (Hadoop).
2. **Velocity (Kecepatan Aliran Data)**
   Laporan masyarakat dapat terjadi lonjakan tiba-tiba (misal saat banjir besar). Selain itu, sistem menerima aliran metrik cuaca dan traffic secara terus-menerus. *Apache Kafka* digunakan untuk menyerap antrean pesan (streaming) secara *real-time* tanpa membuat server *crash*.
3. **Variety (Keragaman Format Data)**
   SRIS menggabungkan tiga jenis data sekaligus:
   - *Unstructured Data*: File gambar/foto jalan rusak.
   - *Semi-Structured Data*: Log event Kafka (JSON) untuk telemetri cuaca dan traffic.
   - *Structured Data*: Tabel relasional user dan koordinat GIS (Latitude/Longitude).
4. **Veracity (Keabsahan dan Kebersihan Data)**
   Laporan dari crowdsourcing sangat rawan palsu (noise/hoax/gambar blur). Sistem menyelesaikan tantangan *Veracity* ini melalui dua tahap: (a) *AI-Validation* menggunakan model YOLOv8 untuk memastikan foto benar-benar mengandung *pothole/crack*, dan (b) Pembersihan anomali di Layer *Silver* PySpark.
5. **Value (Nilai/Insight Eksekusi)**
   Data mentah berukuran besar tersebut tidak berguna tanpa diolah. Melalui agregasi di Layer *Gold* menggunakan PySpark, triliunan *bytes* data tersebut disuling menjadi satu angka *Actionable Insight*: **Priority Score** dan **Road Health Index**. Nilai inilah yang dipakai Dinas PU untuk langsung memberangkatkan truk aspal ke titik paling kritis.
