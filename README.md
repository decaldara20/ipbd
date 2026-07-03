
---

# Big Data Infrastructure: Makroekonomi Sentiment Pipeline

## 📋 Deskripsi Proyek

Proyek ini mengimplementasikan infrastruktur *Big Data end-to-end* untuk pemrosesan berita makroekonomi secara otomatis. Sistem ini menggabungkan *Data Lake* untuk penyimpanan mentah, *Machine Learning* berbasis IndoBERT untuk analisis sentimen, *Data Warehouse* untuk penyajian data, serta dasbor pemantauan (*monitoring*) yang komprehensif.

## 🏗️ Arsitektur Sistem

Sistem ini dirancang menggunakan arsitektur *Medallion* dengan alur sebagai berikut:

1. **Data Source**: *Crawling* berita makroekonomi.
2. **Bronze Layer (Data Lake)**: Penyimpanan file mentah (JSON) di **MinIO**.


3. **Processing Layer**: Orchestrasi otomatis menggunakan **Apache Airflow** yang menjalankan *Batch Inference* dengan model **IndoBERT**.


4. **Silver/Gold Layer (Data Warehouse)**: Data tabular terstruktur di **PostgreSQL**.


5. **Serving & Visualization**: **Metabase** untuk *Business Intelligence* dan **Grafana** untuk infrastruktur *monitoring*.



## 🛠️ Komponen Utama

* **Orchestration**: Apache Airflow.
* **Object Storage**: MinIO (Data Lake).
* **Database**: PostgreSQL (Data Warehouse).
* **Machine Learning**: IndoBERT (Sentiment Analysis).
* **Dashboarding**: Metabase (Analitik) & Grafana (Infrastruktur).
* **Monitoring**: Prometheus & Node Exporter.



## 🚀 Fitur Tata Kelola (Governance & Security)

* **PII Masking**: Otomatis menyensor *email*, nomor telepon, dan NIK menggunakan *Regex* sebelum data masuk ke *database*.


* **Data Quality**: *Profiling* otomatis untuk membuang data *null* dan mencatat *error* pipeline.


* **Audit Trail**: Pencatatan jejak waktu pembuatan/perubahan data dan eksekutor sistem.


* **Alerting**: Notifikasi otomatis via email jika terjadi kegagalan sistem (*pipeline failure*).



## 📊 Monitoring & Observability

Kami menerapkan pemantauan *real-time* untuk memastikan performa infrastruktur:

* **Hardware Monitoring**: Penggunaan CPU, RAM, Disk I/O, dan Network dipantau melalui **Node Exporter** dan divisualisasikan oleh **Grafana**.


* **Log Management**: Logging terpusat melalui Airflow dengan tingkat *severity* (INFO/ERROR) untuk memantau integritas data.



## ⚙️ Cara Menjalankan

1. Pastikan Docker dan Docker Compose terinstall.
2. Jalankan kluster:
```bash
docker compose up -d

```


3. Akses Airflow di `localhost:8080`, Metabase di `localhost:3000`, dan Grafana di `localhost:3001`.

## 📜 Dokumentasi

Proyek ini telah dikembangkan sesuai dengan Instrumen Penilaian Team Based Project (TBP) Infrastruktur Big Data. Seluruh pipeline bersifat *idempotent* dan dapat dijalankan ulang (*reproducible*) kapan saja.

---
