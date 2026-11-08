# To-Do List & Action Plan: Final Project Teknologi Big Data (Kelompok 6)

**Judul Proyek:** Sistem Terpadu Pemantauan Spatio-Temporal Titik Api (_Hotspot_) dan Peringatan Dini Kebakaran Hutan & Lahan di Indonesia Menggunakan Arsitektur Big Data End-to-End  
**Target Nilai:** Sangat Baik / Grade A (85–100 Poin)  
**Tenggat Waktu Final:** Akhir Semester (Sesuai Jadwal EMAS3)

---

## Ringkasan Pembagian Peran Anggota Tim

- **[BRY] Muhammad Bryan Farras (2306230975)**: Lead Architect & Stream Engineer (Kafka + Spark Streaming + Bab 1 & Bab 4)
- **[MRZ] Mirza Adi Raffiansyah (2306210323)**: Storage & Batch Engineer (HBase + Parquet + Hive + Bab 2 & Bab 3)
- **[CLV] Calvin Wirathama Katoroy (2306242395)**: Serving & Backend Engineer (FastAPI + Docker/WSL2 + Benchmark Latensi + Bab 4)
- **[SLM] Salim (2306204604)**: Visualization & Project Lead (Streamlit + Folium GIS + Laporan PDF + PPT + Video YouTube)

---

## DAFTAR TUGAS PER FASE

### Fase 0: Setup Repositori & Lingkungan Kerja (Target: Hari 1–3)

- [ ] **[CLV]** Struktur direktori proyek di `FinalProject/`:
  - `ingestion/` (Kafka producer & data feed)
  - `streaming/` (PySpark structured streaming)
  - `storage/` (HBase scripts & Parquet warehouse)
  - `serving/` (FastAPI backend)
  - `dashboard/` (Streamlit GIS UI)
  - `report/` (Laporan PDF, PPT, assets gambar)
- [ ] **[CLV]** Buat berkas `docker-compose.yml` untuk orkestrasi container:
  - Zookeeper + Apache Kafka 3.x
  - Apache HBase 2.6.7 (dari Topic 9)
  - Apache Hive 4.2.1 (dari Topic 10)
- [ ] **[CLV]** Buat `requirements.txt` Python environment (pyspark, confluent-kafka, fastapi, uvicorn, streamlit, folium, streamlit-folium, plotly).
- [ ] **[MRZ]** Siapkan dataset FIRMS VIIRS skala 100k dan 1 juta baris serta berkas referensi `ref_grid_provinsi.csv`.

---

### Fase 1: Ingestion Layer — Apache Kafka (Target: Hari 4–6)

- [ ] **[BRY]** Buat Kafka Topic `hotspot-stream` dengan konfigurasi partisi optimal (misal 3 partisi).
- [ ] **[BRY]** Bangun skrip `ingestion/kafka_producer.py`:
  - Membaca data titik api satelit secara streaming (simulasi delay realistis per event).
  - Serialisasi payload JSON: `hotspot_id`, `latitude`, `longitude`, `acq_date`, `acq_time`, `confidence`, `frp`, `daynight`.
- [ ] **[BRY]** Implementasikan mekanisme _health check_ dan pengukuran throughput pesan (events/detik).
- [ ] **[BRY]** Ambil screenshot terminal WSL: Kafka broker berjalan, topic aktif, dan pesan terkirim.

---

### Fase 2: Processing Layer — Hybrid Streaming & Batch (Target: Hari 7–11)

#### A. Speed Layer (Real-time Stream)

- [ ] **[BRY]** Bangun skrip `streaming/spark_streaming.py`:
  - Konsumsi data dari Kafka Topic `hotspot-stream` via Spark Structured Streaming.
  - Terapkan _Watermarking_ dan _Sliding Window_ (misal window 10 menit, slide 2 menit).
  - Filter titik api bahaya tinggi (`frp >= 20 MW` & `confidence = 'high'`).
  - Broadcast Join di memori dengan `ref_grid_provinsi.csv` untuk penentuan provinsi otomatis.
- [ ] **[MRZ]** Bangun sink penulisan stream ke **Apache HBase**:
  - Tulis event titik api ke tabel `hotspot` dengan rowkey unik: `grid#tanggal_terbalik#jam#id`.

#### B. Batch Layer (Analitik Historis Skala Besar)

- [ ] **[MRZ]** Bangun pipeline penulisan batch ke format **Apache Parquet**:
  - Partisi berkas berdasarkan `acq_date` (`PARTITIONED BY (acq_date)`).
  - Terapkan kompresi Snappy dan optimasi ukuran partisi.
- [ ] **[MRZ]** Eksekusi kueri analitik berkala di **Spark SQL & Hive**:
  - Agregasi risiko provinsi mingguan/bulanan.
  - Perhitungan akumulasi energi FRP per tipe vegetasi/gambut.
- [ ] **[MRZ]** Ambil screenshot terminal WSL: PySpark Streaming logs, HBase table put, dan output query Spark SQL.

---

### Fase 3: Serving Layer — FastAPI Backend (Target: Hari 12–14)

- [ ] **[CLV]** Bangun layanan REST API di `serving/main.py` menggunakan FastAPI:
  - `GET /api/v1/hotspots/recent`: Mengambil 50 titik api terbaru dari HBase (<1 detik).
  - `GET /api/v1/alerts/critical`: Mengambil daftar titik api aktif berkategori bahaya ekstrem (FRP > 50 MW).
  - `GET /api/v1/analytics/provinces`: Mengambil rekapitulasi data agregasi historis per provinsi.
  - `GET /api/v1/health`: Cek status konektivitas Kafka, HBase, dan Spark.
- [ ] **[CLV]** Uji endpoint via Swagger UI (`http://localhost:8000/docs`).
- [ ] **[CLV]** Ambil screenshot antarmuka Swagger dan respon JSON endpoint.

---

### Fase 4: Visualization & Prescriptive Insight Layer (Target: Hari 15–18)

- [ ] **[SLM]** Bangun antarmuka dashboard di `dashboard/app.py` menggunakan Streamlit:
  - **Peta Interaktif Folium GIS**:
    - Heatmap intensitas radiasi panas FRP.
    - Marker Clustering titik api yang dapat di-klik untuk detail satelit 375m.
    - Layer poligon 239 sel grid dengan pewarnaan zona bahaya (Hijau, Kuning, Merah).
  - **Live Alert Banner**: Notifikasi peringatan dini jika sel grid terdeteksi lonjakan titik api.
  - **Grafik Analitik Plotly**: Tren temporal harian dan Top 10 Provinsi terdampak karhutla.
- [ ] **[SLM]** Bangun **Modul Analisis Preskriptif (Kunci Nilai A 10%)**:
  - Algoritma rekomendasi prioritas pemadaman berdasarkan indeks gabungan: _Kepadatan Titik Api_ + _FRP Puncak_ + _Luas Gambut_.
  - Rekomendasi alokasi unit helikopter _water bombing_ BNPB dan regu Manggala Agni.
- [ ] **[SLM]** Ambil screenshot dashboard beresolusi tinggi untuk lampiran laporan.

---

### Fase 5: Pengujian End-to-End & Benchmarking (Target: Hari 19–21)

- [ ] **[TIM]** Jalankan skenario simulasi end-to-end:
  - `Feed Satelit` -> `Kafka (Ingest)` -> `Spark Streaming` -> `HBase/Parquet` -> `FastAPI` -> `Streamlit Dashboard`.
- [ ] **[CLV & BRY]** Ukur latensi end-to-end: dari data masuk ke Kafka hingga muncul di peta dashboard (<3 detik).
- [ ] **[MRZ]** Ukur throughput batch vs streaming pada skala 10k, 100k, dan 1 juta baris.
- [ ] **[TIM]** Pastikan tidak ada _error fatal_, _memory leak_, atau kegagalan _OutOfMemory_ (OOM).

---

### Fase 6: Penyusunan Laporan, Slide PPT, & Video (Target: Hari 22–25)

- [ ] **[MRZ & SLM]** Tulis Laporan Akhir (PDF) format 5 Bab sesuai panduan:
  - Abstrak
  - Bab 1: Pendahuluan (Latar Belakang Karhutla, Rumusan Masalah, Tujuan)
  - Bab 2: Tinjauan Pustaka & Justifikasi 4V Big Data
  - Bab 3: Metodologi & Diagram Arsitektur Solusi
  - Bab 4: Implementasi Teknis, Cuplikan Kode, Screenshot Sistem, & Pembahasan Wawasan
  - Bab 5: Kesimpulan & Saran
  - Daftar Pustaka
- [ ] **[SLM]** Susun slide presentasi (PPT) profesional:
  - Ringkasan masalah, diagram arsitektur, demo cuplikan sistem, insight preskriptif.
- [ ] **[TIM]** Rekam video presentasi dan demo aplikasi end-to-end (durasi 10–15 menit).
- [ ] **[SLM]** Unggah video ke YouTube dan cantumkan tautan pada slide terakhir PPT.
- [ ] **[SLM]** Kompilasi file Laporan PDF dan PPT ke dalam berkas arsip `.zip` / `.rar` dan unggah ke EMAS3 sebelum batas akhir.
