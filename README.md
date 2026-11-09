# Sistem Terpadu Pemantauan Spatio-Temporal Titik Api (_Hotspot_) & Peringatan Dini Kebakaran Hutan di Indonesia

### _End-to-End Big Data Architecture: Apache Kafka, Spark Streaming, HBase, Parquet, Hive, FastAPI, & Streamlit GIS_

[![Course](https://img.shields.io/badge/Mata%20Kuliah-Teknologi%20Big%20Data-blue.svg)](https://emas3.ui.ac.id)
[![Project](https://img.shields.io/badge/Tugas-Final%20Project-success.svg)](#)
[![Architecture](https://img.shields.io/badge/Architecture-Lambda%20%2F%20Hybrid-orange.svg)](#)
[![Status](https://img.shields.io/badge/Status-Active%20Development-brightgreen.svg)](#)

---

## Identitas Kelompok 6

| No  | Nama Anggota                 |    NPM     | Peran & Tanggung Jawab Teknis                                                                                               |
| :-: | ---------------------------- | :--------: | --------------------------------------------------------------------------------------------------------------------------- |
|  1  | **Muhammad Bryan Farras**    | 2306230975 | **Lead Architect & Stream Engineer**<br>Apache Kafka Ingestion, Spark Structured Streaming, Bab 1 & Bab 4 Laporan           |
|  2  | **Mirza Adi Raffiansyah**    | 2306210323 | **Storage & Batch Engineer**<br>Apache HBase 2.6.7, Parquet Partitioned Lake, Spark SQL / Hive, Bab 2 & Bab 3 Laporan       |
|  3  | **Calvin Wirathama Katoroy** | 2306242395 | **Serving & Backend Engineer**<br>FastAPI REST Service, Docker & WSL2 Infrastructure, Latency Benchmark, Bab 4 Laporan      |
|  4  | **Salim**                    | 2306204604 | **Visualization & Project Lead**<br>Streamlit Interactive GIS Dashboard, Prescriptive Analysis, Laporan Akhir, PPT, & Video |

---

## Ringkasan Eksekutif & Latar Belakang

Kebakaran hutan dan lahan (karhutla) di Indonesia merupakan bencana lingkungan menahun yang mengakibatkan kerusakan keanekaragaman hayati ekosistem gambut, kerugian ekonomi miliaran dolar, serta polusi asap lintas batas negara (_transboundary haze_). Satelit penginderaan jauh modern **NASA FIRMS VIIRS (Visible Infrared Imaging Radiometer Suite)** pada satelit Suomi-NPP dan NOAA-20 menghasilkan pembacaan anomali radiasi termal resolusi tinggi (375m) secara kontinu di seluruh kawasan kepulauan Indonesia.

Proyek ini membangun solusi terintegrasi hulu-ke-hilir (**end-to-end Big Data system**) yang:

1. **Mengonsumsi aliran telemetri satelit berkecepatan tinggi** menggunakan **Apache Kafka**.
2. **Memproses penyaringan anomali panas ekstrem secara real-time** menggunakan **Spark Structured Streaming** untuk sistem peringatan dini dalam hitungan detik.
3. **Menyediakan penyimpanan hibrida ganda**:
   - _Storage Operasional_: **Apache HBase** untuk pencarian titik api peta berlatensi sub-detik (<1 detik).
   - _Storage Analitik (Data Lake)_: **Apache Parquet (Snappy)** berpartisi tanggal (`acq_date`) dengan **Hive Metastore** untuk analisis agregasi multi-tahun.
4. **Menyajikan dashboard analitik interaktif berbasis geospasial** menggunakan **FastAPI**, **Streamlit**, dan **Folium GIS** yang dilengkapi **rekomendasi preskriptif** alokasi satgas Manggala Agni dan helikopter _water bombing_ BNPB.

---

## Justifikasi Karakteristik Big Data (The 4V's)

Mengapa kasus ini **TIDAK DAPAT** diselesaikan secara efisien menggunakan basis data relasional konvensional (RDBMS seperti MySQL/PostgreSQL):

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 THE 4V's BIG DATA JUSTIFICATION                                  │
├─────────────────┬───────────────────────────────────┬───────────────────────────────────────────┤
│ Dimensi         │ Karakteristik Data Kasus          │ Mengapa RDBMS Konvensional Gagal?         │
├─────────────────┼───────────────────────────────────┼───────────────────────────────────────────┤
│ 1. VOLUME       │ Akumulasi historis 10 tahun data  │ Indeks B-Tree membengkak; query agregasi  │
│                 │ satelit VIIRS mencapai ±85 juta   │ multi-tahun mengalami disk thrashing dan  │
│                 │ baris (>100 GB).                  │ crash/timeout. Diatasi dengan Parquet.    │
├─────────────────┼───────────────────────────────────┼───────────────────────────────────────────┤
│ 2. VELOCITY     │ Feed satelit masuk kontinu ribuan │ RDBMS mengalami write lock contention saat│
│                 │ deteksi per pass orbit; butuh     │ menerima data kontinu dan melayani query. │
│                 │ deteksi bahaya <2 detik.          │ Diatasi dengan Apache Kafka buffer.       │
├─────────────────┼───────────────────────────────────┼───────────────────────────────────────────┤
│ 3. VARIETY      │ Koordinat spatio-temporal, sensor │ Operasi spatial join skala besar di RDBMS │
│                 │ termal FRP, digabung 239 sel grid │ sangat lambat; diatasi dengan Spark       │
│                 │ dan poligon lahan gambut rawan.   │ in-memory Broadcast Hash Join.            │
├─────────────────┼───────────────────────────────────┼───────────────────────────────────────────┤
│ 4. VERACITY     │ Banyak false alarm (pantulan atap │ Filter bertingkat di stream: validasi     │
│                 │ pabrik, cerobong flare industri,  │ confidence nominal/high dan ambang        │
│                 │ anomali sensor berskala rendah).  │ batas radiasi termal (FRP >= 20 MW).      │
└─────────────────┴───────────────────────────────────┴───────────────────────────────────────────┘
```

---

## Arsitektur Solusi (Lambda / Hybrid Architecture)

## ![architecture](misc/Hybrid%20Architecture.svg)

## Tech Stack & Alat yang Digunakan

| Lapisan Sistem          | Teknologi / Alat                     |      Versi      | Peran Utama dalam Sistem                                                                      |
| ----------------------- | ------------------------------------ | :-------------: | --------------------------------------------------------------------------------------------- |
| **Ingestion**           | **Apache Kafka**                     |     `3.7.0`     | Message broker penampung data aliran telemetri satelit berkecepatan tinggi.                   |
| **Stream Processing**   | **Spark Structured Streaming**       |     `4.2.0`     | Pemrosesan stream mikro-batch di memori untuk deteksi kebakaran berlatensi rendah.            |
| **Batch Processing**    | **Spark SQL & Hive (Tez)**           | `4.2.0 / 4.2.1` | Analisis batch historis, join batas administrasi, dan kalkulasi risiko wilayah.               |
| **Storage Operasional** | **Apache HBase**                     |     `2.6.7`     | Penyimpanan NoSQL berlatensi sub-detik untuk melayani permintaan klik dan peta operasional.   |
| **Storage Analitik**    | **Apache Parquet (Snappy)**          |  `Format 2.x`   | Penyimpanan kolumnar efisien terkompresi dengan _partition pruning_ harian.                   |
| **Serving Layer**       | **FastAPI**                          |    `0.110+`     | REST API asinkron performa tinggi sebagai jembatan data ke antarmuka visual.                  |
| **Visualisasi GIS**     | **Streamlit & Folium GIS**           | `1.32+ / 0.16+` | Dashboard web interaktif dengan peta _Heatmap_, _Marker Clustering_, dan wawasan preskriptif. |
| **Lingkungan Kerja**    | **WSL2 (Ubuntu 22.04 LTS) & Docker** |    `29.1.3`     | Ekosistem eksekusi Linux native terisolasi dengan alokasi RAM dan vCPU terkontrol.            |

---

## Struktur Repositori

```text
FinalProject/
├── README.md                            <- Berkas dokumentasi utama proyek
├── PROPOSAL_RANCANGAN_TUGAS_AKHIR.md    <- Dokumen lengkap rancangan & justifikasi teknis
├── TODO.md                              <- To-Do List & action plan 7 fase tim
├── docker-compose.yml                   <- Orkestrasi multi-container (Kafka, HBase, Hive)
├── requirements.txt                     <- Dependensi Python environment
│
├── ingestion/                           <- Modul Ingestion (Apache Kafka)
│   ├── kafka_producer.py                <- Simulator pengiriman data satelit ke Kafka Topic
│   └── verify_kafka.sh                  <- Skrip verifikasi topik dan throughput pesan
│
├── streaming/                           <- Modul Stream Processing (PySpark)
│   ├── spark_streaming.py               <- Pipeline Spark Structured Streaming real-time
│   └── broadcast_lookup.py              <- Broadcast join tabel referensi grid ke memori
│
├── storage/                             <- Modul Dual Storage (HBase & Parquet)
│   ├── hbase_sink.py                    <- Handler penulisan stream ke tabel HBase
│   ├── build_parquet_lake.py            <- Pipeline konversi batch ke Parquet berpartisi
│   └── schema/                          <- DDL skema HBase & HiveQL
│
├── serving/                             <- Modul Serving REST API (FastAPI)
│   ├── main.py                          <- Endpoint REST API (Hotspots, Alerts, Analytics)
│   └── routers/                         <- Modular API routes & serializers
│
├── dashboard/                           <- Modul Visualisasi (Streamlit & Folium GIS)
│   ├── app.py                           <- Aplikasi utama Streamlit
│   ├── components/                      <- Modul peta Folium, heatmap, & grafik Plotly
│   └── prescriptive/                    <- Logika rekomendasi penanggulangan karhutla
│
├── data/                                <- Dataset & tabel referensi
│   ├── hotspot_sample.csv               <- Sampel data telemetri satelit VIIRS
│   └── ref_grid_provinsi.csv            <- Tabel dimensi 239 sel grid 1°x1° provinsi
│
├── document emas/                       <- Arsip pedoman resmi perkuliahan
│   └── 12 - Panduan Tugas Akhir.pdf     <- Panduan Tugas Akhir EMAS3
│
└── report/                              <- Berkas Laporan Akhir & Publikasi
    ├── Tugas_Akhir_Kelompok6.pdf        <- Laporan final PDF (5 Bab standar)
    ├── Tugas_Akhir_Kelompok6.pptx       <- Slide presentasi (link YouTube pada slide akhir)
    └── shots/                           <- Screenshot bukti sistem berjalan
```

## Panduan Menjalankan Sistem (Step-by-Step)

### 1. Prasyarat Sistem

- Windows 11 dengan **WSL2 (`Ubuntu-22.04 LTS`)** aktif.
- RAM minimal 8 GB (disarankan 16 GB), disk kosong minimal 15 GB.
- Docker Desktop atau Docker Engine di WSL2.
- Python 3.10+ dan OpenJDK 17+.

### 2. Setup Lingkungan & Container

```bash
# Masuk ke lingkungan WSL2
wsl -d Ubuntu-22.04 -u root
cd /mnt/c/Users/asus/Documents/BigData/FinalProject

# Pasang dependensi Python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Nyalakan stack container (Kafka, Zookeeper, HBase)
docker compose up -d
docker ps
```

### 3. Jalankan Pipeline Ingestion (Kafka Producer)

```bash
python3 ingestion/kafka_producer.py --topic hotspot-stream --rate 50
```

### 4. Jalankan Spark Structured Streaming

```bash
python3 streaming/spark_streaming.py
```

### 5. Jalankan REST Serving API

```bash
uvicorn serving.main:app --host 0.0.0.0 --port 8000 --reload
# Dokumentasi Swagger: http://localhost:8000/docs
```

### 6. Buka Dashboard Interaktif Streamlit

```bash
streamlit run dashboard/app.py --server.port 8501
# Akses antarmuka: http://localhost:8501
```

---

## Fitur Unggulan & Analisis Preskriptif (Kriteria Nilai A)

Untuk memenuhi rubrik nilai tertinggi (**Visualisasi Interaktif & Analisis Preskriptif Mendalam**), dashboard menyajikan fitur keputusan:

1. **Peta Folium Spatio-Temporal Interaktif**:
   - _Heatmap Radiasi Termal (FRP)_ untuk melihat zona api terpadat.
   - _Marker Clustering_ titik api dengan pop-up koordinat satelit 375m dan estimasi daya api (MW).
   - _Choropleth Layer_ 239 sel grid dengan pewarnaan zona kerawanan (_Hijau = Rendah, Kuning = Sedang, Merah = Bahaya Ekstrem_).
2. **Real-time Alert Ticker**: Notifikasi pop-up seketika saat terdeteksi titik api berintensitas tinggi (>50 MW) di dekat kawasan lahan gambut.
3. **Sistem Rekomendasi Preskriptif (_Decision Support System_)**:
   - **Prioritas Pemadaman**: Perankingan otomatis sel grid darurat berdasarkan indeks komposit: _Daya FRP Puncak_ + _Kepadatan Titik Api_ + _Kedekatan Kubah Gambut_.
   - **Alokasi Sumber Daya**: Estimasi kebutuhan alokasi unit helikopter _water bombing_ BNPB dan penempatan satgas darat Manggala Agni.

---

## Matriks Pemenuhan Rubrik Penilaian (Target: 100%)

| Komponen Penilaian                  |  Bobot  |   Target Kategori   | Strategi Pemenuhan pada Proyek Kelompok 6                                                                                                 |
| ----------------------------------- | :-----: | :-----------------: | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Definisi Masalah & Justifikasi**  | **15%** | **Sangat Baik (A)** | Justifikasi 4V terukur pada data satelit VIIRS NASA FIRMS; argumen kegagalan RDBMS dibuktikan secara kuantitatif.                         |
| **Arsitektur & Metodologi**         | **25%** | **Sangat Baik (A)** | Diagram arsitektur Lambda detail memisahkan Ingestion (Kafka), Processing (Spark), Dual Storage (HBase & Parquet), dan Serving (FastAPI). |
| **Implementasi Teknis (Coding)**    | **40%** | **Sangat Baik (A)** | Pipeline end-to-end berjalan sempurna dari feed satelit hingga UI; optimasi partisi Parquet, broadcast join, clean code, dan modular.     |
| **Visualisasi & Wawasan (Insight)** | **10%** | **Sangat Baik (A)** | Dashboard Streamlit + Folium interaktif dan menyajikan wawasan preskriptif (rekomendasi tindakan penanggulangan karhutla).                |
| **Laporan & Presentasi**            | **10%** | **Sangat Baik (A)** | Laporan PDF 5 Bab terstruktur rapi, slide PPT profesional, dan rekaman video presentasi YouTube disertakan.                               |

---

## 🔗 Tautan & Dokumen Terkait

- 📄 **[Proposal Rancangan Lengkap](document/PROPOSAL_RANCANGAN_TUGAS_AKHIR.md)** — Dokumen perancangan rinci, justifikasi teknis, dan skema data.
- 📋 **[To-Do List & Action Plan Per Fase](document/TODO.md)** — Pelacak progres tugas 7 fase terstruktur.
- 📜 **[Panduan Tugas Akhir Resmi](document/12%20-%20Panduan%20Tugas%20Akhir.pdf)** — Kerangka acuan dan rubrik penilaian mata kuliah.
