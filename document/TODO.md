# To-Do List & Action Plan: Final Project Teknologi Big Data

**Judul Proyek:** Sistem Terpadu Pemantauan Spatio-Temporal Titik Api (_Hotspot_) dan Peringatan Dini Kebakaran Hutan & Lahan di Indonesia Menggunakan Arsitektur Big Data End-to-End  
**Target Kategori:** Sangat Baik / Grade A (85–100 Poin)  
**Kerangka Waktu:** 7 Fase Terstruktur Menuju Pengumpulan Akhir Semester

---

## DAFTAR TUGAS PER FASE

---

### Fase 0: Setup Repositori, Environment, & Dataset

**Tujuan:** Menyiapkan fondasi sistem, struktur direktori modular, dependensi, dan kontainer terisolasi sebelum implementasi kode.

- [x] **Struktur Repositori Proyek:**
  - Buat folder modular di direktori kerja:
    - `ingestion/` (Kafka producer & data feed)
    - `streaming/` (PySpark structured streaming)
    - `storage/` (HBase scripts & Parquet lake)
    - `serving/` (FastAPI backend service)
    - `dashboard/` (Streamlit GIS frontend)
    - `data/` (Dataset satelit & tabel referensi)
    - `report/` (Laporan akhir, slide presentasi, screenshots)
- [x] **Orkestrasi Lingkungan Kontainer (`docker-compose.yml`):**
  - Konfigurasi service **Apache Zookeeper** & **Apache Kafka 3.x**
  - Konfigurasi service **Apache HBase 2.6.7** (port 16010 & 9090)
  - Konfigurasi service **Apache Hive 4.2.1** (port 10000 & 10002)
- [x] **Manajemen Dependensi Python (`requirements.txt`):**
  - Instalasi paket: `pyspark`, `confluent-kafka`, `fastapi`, `uvicorn`, `streamlit`, `folium`, `streamlit-folium`, `plotly`, `pandas`, `pyarrow`.
- [x] **Penyiapan & Verifikasi Dataset:**
  - Ekstraksi dataset satelit VIIRS NASA FIRMS skala `100.000` baris dan proyeksi `1.000.000` baris.
  - Penyiapan tabel dimensi `ref_grid_provinsi.csv` (239 sel grid 1°×1°).

_Deliverable Fase 0:_ Stack Docker aktif di port masing-masing, lingkungan virtualenv Python terpasang, dan dataset siap diproses.

---

### Fase 1: Lapisan Ingestion — Apache Kafka

**Tujuan:** Membangun _message broker_ terdistribusi untuk menerima dan menampung aliran data telemetri satelit berkecepatan tinggi tanpa _loss_.

- [ ] **Konfigurasi Topic Kafka:**
  - Buat topic `hotspot-stream` dengan partisi yang memadai (minimal 3 partisi).
  - Tentukan kebijakan retensi data (_retention policy_) dan _replication factor_.
- [ ] **Pengembangan Simulator Feed Satelit (`ingestion/kafka_producer.py`):**
  - Buat skrip _producer_ yang membaca dataset titik api dan memancarkannya secara streaming.
  - Atur laju pengiriman (_ingestion rate_, misal 50–200 pesan/detik).
  - Serialisasi format payload JSON:
    - `hotspot_id`, `latitude`, `longitude`, `acq_date`, `acq_time`, `confidence`, `frp`, `daynight`.
- [ ] **Verifikasi & Monitoring Ingestion:**
  - Uji konsumsi pesan via console consumer CLI.
  - Catat metrik throughput pengiriman (pesan/detik).
  - Dokumentasikan screenshot terminal WSL: broker Kafka aktif, topic terbentuk, dan stream berjalan.

_Deliverable Fase 1:_ Aliran data satelit mengalir stabil ke broker Kafka dan siap dikonsumsi oleh lapisan pemrosesan.

---

### Fase 2: Lapisan Processing — Hybrid Streaming & Batch

**Tujuan:** Mengolah data aliran secara instan untuk peringatan dini (Speed Layer) dan memproses agregasi historis skala besar (Batch Layer).

#### A. Speed Layer (Real-time Stream Processing)

- [ ] **Pengembangan Engine Streaming (`streaming/spark_streaming.py`):**
  - Bangun consumer Spark Structured Streaming dari topic `hotspot-stream`.
  - Terapkan mekanisme _Watermarking_ untuk menangani data yang datang terlambat (_late data_).
  - Terapkan _Sliding Window_ agregasi (misal window 10 menit, slide 2 menit).
- [ ] **Filter Anomali & Peringatan Dini:**
  - Saring titik api berkategori bahaya ekstrem (`confidence = 'high'` dan `frp >= 20.0 MW`).
  - Lakukan _in-memory Broadcast Join_ dengan tabel `ref_grid_provinsi.csv` untuk memetakan nama provinsi secara instan.
- [ ] **Sink Penulisan Stream ke Storage Operasional:**
  - Tulis event titik api terbaru ke **Apache HBase** (tabel `hotspot`) dengan pola rowkey: `grid#tanggal_terbalik#jam#id`.

#### B. Batch Layer (Analitik Historis Skala Besar)

- [ ] **Pipeline Data Lake Parquet (`storage/build_parquet_lake.py`):**
  - Tulis akumulasi data mentah ke format kolumnar **Apache Parquet**.
  - Terapkan partisi berdasarkan tanggal (`PARTITIONED BY (acq_date)`).
  - Terapkan kompresi Snappy untuk meminimalkan ukuran disk.
- [ ] **Kueri Analitik Batch Terjadwal (Spark SQL & Apache Hive):**
  - Jalankan kueri agregasi total dan rata-rata FRP per provinsi.
  - Hitung matriks akumulasi energi panas pada kawasan lahan gambut.
- [ ] **Dokumentasi Bukti Teknis:**
  - Ambil screenshot terminal WSL: log eksekusi Spark Streaming, record yang masuk ke HBase, dan hasil kueri analitik Hive/Spark SQL.

_Deliverable Fase 2:_ Pipeline streaming menyaring anomali secara real-time ke HBase dan data analitik tersimpan rapi di Parquet Data Lake.

---

### Fase 3: Lapisan Serving — REST API Backend (FastAPI)

**Tujuan:** Menyediakan antarmuka API performa tinggi yang menghubungkan lapisan penyimpanan data ke antarmuka visual pengguna.

- [ ] **Inisialisasi Service FastAPI (`serving/main.py`):**
  - Bangun arsitektur API modular dengan FastAPI dan Uvicorn.
  - Konfigurasi middleware CORS agar dapat diakses oleh dashboard visual.
- [ ] **Implementasi Endpoint Operasional & Analitik:**
  - `GET /api/v1/hotspots/recent`: Mengambil 50 titik api terbaru dari HBase (<1 detik).
  - `GET /api/v1/alerts/critical`: Mengambil daftar titik api aktif berstatus darurat (_high confidence_ & FRP tinggi).
  - `GET /api/v1/analytics/provinces`: Mengambil rangkuman agregasi jumlah titik api dan total FRP per provinsi.
  - `GET /api/v1/analytics/trends`: Mengambil tren harian/mingguan intensitas kebakaran.
  - `GET /api/v1/health`: Memeriksa status kesehatan koneksi HBase, Kafka, dan Spark.
- [ ] **Pengujian & Validasi API:**
  - Uji seluruh endpoint melalui antarmuka Swagger UI (`http://localhost:8000/docs`).
  - Catat waktu respons endpoint (target: rata-rata respons <150 ms).
  - Dokumentasikan screenshot antarmuka Swagger dan respon payload JSON.

_Deliverable Fase 3:_ Layanan REST API aktif, teruji, dan menyediakan data siap saji untuk dashboard visual.

---

### Fase 4: Lapisan Visualisasi & Wawasan Preskriptif — Streamlit & Folium GIS

**Tujuan:** Menyajikan dashboard geospasial interaktif serta wawasan preskriptif yang dapat dijadikan dasar pengambilan keputusan mitigasi karhutla.

- [ ] **Pengembangan Dashboard GIS Interaktif (`dashboard/app.py`):**
  - Integrasikan Streamlit dengan library pemetaan **Folium** / **Leaflet**.
  - **Peta Spatio-Temporal Interaktif:**
    - _Heatmap Layer_: Menampilkan visualisasi konsentrasi panas kebakaran hutan.
    - _Marker Clustering Layer_: Pengelompokan titik api dinamis yang dapat di-_zoom_ hingga level resolusi satelit 375m.
    - _Choropleth Grid Layer_: Pewarnaan 239 sel grid berdasarkan zona risiko (Hijau = Rendah, Kuning = Waspada, Merah = Bahaya Ekstrem).
- [ ] **Panel Peringatan Dini Real-Time (Early Warning Ticker):**
  - Buat komponen visual notifikasi peringatan seketika saat terdeteksi lonjakan api ekstrem di wilayah sensitif.
- [ ] **Grafik Analitik & Metrik Kunci (Plotly):**
  - Diagram batang Top 10 Provinsi terdampak karhutla terparah.
  - Grafik garis tren radiasi termal (FRP) harian.
- [ ] **Modul Analisis Preskriptif (Kunci Nilai A):**
  - Implementasikan algoritma perankingan sel prioritas penanganan berdasarkan formula gabungan: _Kepadatan Titik Api_ + _FRP Puncak_ + _Luas Lahan Gambut_.
  - Sajikan rekomendasi alokasi unit helikopter _water bombing_ BNPB dan regu pemadam darat Manggala Agni.
- [ ] **Dokumentasi Tampilan Visual:**
  - Ambil tangkapan layar antarmuka dashboard beresolusi tinggi untuk laporan dan presentasi.

_Deliverable Fase 4:_ Dashboard interaktif berbasis web berjalan sempurna dengan peta GIS real-time dan modul rekomendasi tindakan mitigasi.

---

### Fase 5: Pengujian End-to-End, Latensi, & Benchmarking

**Tujuan:** Memvalidasi keandalan, stabilitas, dan performa arsitektur secara keseluruhan dari hulu ke hilir.

- [ ] **Simulasi Aliran End-to-End:**
  - Uji alur lengkap: `Data Satelit` ➔ `Kafka Topic` ➔ `Spark Streaming` ➔ `HBase & Parquet` ➔ `FastAPI` ➔ `Streamlit UI`.
- [ ] **Pengukuran Latensi End-to-End:**
  - Ukur jeda waktu dari saat titik api dipancarkan di Kafka hingga muncul di peta dashboard (target: <3 detik).
- [ ] **Pengujian Skalabilitas Volume Data:**
  - Uji performa pemrosesan batch pada variasi skala data: `10.000` baris, `100.000` baris, dan `1.000.000` baris.
  - Catat konsumsi memori RAM dan utilisasi vCPU.
- [ ] **Audit Kestabilan Sistem:**
  - Pastikan tidak ada kegagalan _OutOfMemory_ (OOM), _data loss_, atau kebocoran koneksi.

_Deliverable Fase 5:_ Data hasil pengukuran performa, tabel komparasi latensi, dan jaminan sistem berjalan bebas error fatal.

---

### Fase 6: Penyusunan Laporan Akhir, PPT, Video, & Pengumpulan

**Tujuan:** Mendokumentasikan seluruh hasil karya ilmiah dan teknis sesuai dengan sistematika resmi panduan EMAS3.

- [ ] **Penyusunan Laporan Akhir Format PDF (Struktur 5 Bab Standar):**
  - **Halaman Judul & Identitas Kelompok**
  - **Abstrak** (Bahasa Indonesia & Bahasa Inggris)
  - **Bab 1: Pendahuluan** (Latar Belakang Karhutla, Rumusan Masalah, Tujuan)
  - **Bab 2: Tinjauan Pustaka & Justifikasi Big Data** (Teori 4V, Kegagalan RDBMS, Tech Stack)
  - **Bab 3: Metodologi & Perancangan Sistem** (Diagram Arsitektur Lambda, Skema Data, Pipeline)
  - **Bab 4: Implementasi & Pembahasan Hasil** (Snippets kode inti, screenshot sistem berjalan, analisis performa, wawasan preskriptif)
  - **Bab 5: Kesimpulan & Saran**
  - **Daftar Pustaka**
- [ ] **Penyusunan Slide Presentasi (PPT):**
  - Slide profesional yang memuat: Latar belakang masalah, arsitektur solusi, demo cuplikan sistem, grafik performa, dan kesimpulan.
- [ ] **Perekaman Video Pemaparan (YouTube):**
  - Rekam presentasi tim dan demonstrasi aplikasi end-to-end (durasi 10–15 menit).
  - Unggah video ke YouTube dan sematkan tautan video pada slide terakhir PPT.
- [ ] **Pengemasan & Pengumpulan Berkas:**
  - Satukan berkas Laporan PDF dan slide PPT ke dalam satu file arsip (`.zip` atau `.rar`).
  - Unggah berkas arsip ke portal EMAS3 sebelum batas akhir pengumpulan.

_Deliverable Fase 6:_ Berkas PDF laporan final, slide PPT dengan tautan YouTube, dan arsip siap kumpul yang memenuhi 100% rubrik penilaian.
