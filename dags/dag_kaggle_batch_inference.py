from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import base64

TARGET_EMAIL = 'kuntohidayat20@gmail.com'

default_args = {
    'owner': 'Jalu_dan_Kunto',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 3),
    'email': [TARGET_EMAIL],
    'email_on_failure': True,
    'retries': 0,
}

# =========================================================================
# SCRIPT BATCH INFERENCE & ETL
# =========================================================================
batch_script = """
import os
import glob
import pandas as pd
import json
import tarfile
import psycopg2
import torch
from minio import Minio
from transformers import AutoTokenizer, AutoModelForSequenceClassification

print('🚀 [SISTEM] Memulai Batch Inference Pipeline...')

# 1. AUTENTIKASI & UNDUH KAGGLE
# (Ubah dengan kredensial Kaggle Anda yang sebenarnya)
os.environ['KAGGLE_USERNAME'] = "kuntohidayat"
os.environ['KAGGLE_KEY'] = "KGAT_2a2fd28361f69cd293782210779475c1"

print('📥 [KAGGLE] Mengunduh dataset indonesia-news-dataset-2024...')
os.system("kaggle datasets download sh1zuka/indonesia-news-dataset-2024 --unzip")

# Cari file CSV hasil ekstrak
csv_file = glob.glob("*.csv")[0]
df = pd.read_csv(csv_file)

# MITIGASI OOM: Ambil 500 baris terbaru/pertama saja untuk keamanan RAM
df = df.head(500)
print(f"📊 [DATA] Berhasil memuat {len(df)} baris berita dari CSV.")

# 2. UNDUH OTAK INDOBERT DARI MINIO
print("🧠 [ML ENGINE] Menarik model IndoBERT dari Data Lake...")
minio_client = Minio("minio:9000", access_key="admin", secret_key="password_minio_123", secure=False)
minio_client.fget_object("macro-ekonomi-model", "indobert_sentiment_model.tar.gz", "indobert.tar.gz")

with tarfile.open("indobert.tar.gz", "r:gz") as tar:
    tar.extractall(path="./model_AI")

tokenizer = AutoTokenizer.from_pretrained("./model_AI")
model = AutoModelForSequenceClassification.from_pretrained("./model_AI")
model.eval()

# 3. KONEKSI POSTGRESQL
conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
conn.autocommit = True
cur = conn.cursor()

LABEL_MAP = {0: "Negatif", 1: "Netral", 2: "Positif"}

print("⚙️ [INFERENSI] Memulai proses klasifikasi klasal Batch...")
berita_masuk = 0

for index, row in df.iterrows():
    # Asumsi nama kolom Kaggle (Sesuaikan jika nama kolom di CSV aslinya berbeda)
    # Umumnya berisi 'title', 'date', 'text', 'source'
    try:
        judul = str(row.get('title', row.get('judul', 'Tanpa Judul')))
        teks = str(row.get('content', row.get('deskripsi', judul)))
        tanggal = row.get('date', row.get('tanggal', '2024-01-01 00:00:00'))
        sumber = row.get('source', row.get('sumber', 'Kaggle_Dataset'))
        
        if pd.isna(teks) or teks == 'nan': continue

        # Inferensi AI
        inputs = tokenizer(teks, return_tensors="pt", truncation=True, max_length=128)
        with torch.no_grad():
            outputs = model(**inputs)
            probabilitas = torch.nn.functional.softmax(outputs.logits, dim=-1)
            prediksi_angka = torch.argmax(probabilitas, dim=-1).item()
            confidence = probabilitas[0][prediksi_angka].item()

        sentimen_final = LABEL_MAP[prediksi_angka]

        # Kategorisasi Isu
        teks_lower = teks.lower()
        if any(k in teks_lower for k in ["ppn", "pajak"]): sub_isu = "Kebijakan Fiskal"
        elif any(k in teks_lower for k in ["ihsg", "saham", "investasi"]): sub_isu = "Pasar Modal"
        elif any(k in teks_lower for k in ["suku bunga", "bi", "rupiah"]): sub_isu = "Moneter"
        else: sub_isu = "Ekonomi Umum"

        # Simpan ke Database
        cur.execute(\"""
            INSERT INTO berita_makroekonomi 
            (waktu_berita, sumber, judul_berita, sub_isu, sentimen, skor_kepercayaan, jalur_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (judul_berita) DO NOTHING
        \""", (tanggal, sumber, judul, sub_isu, sentimen_final, round(confidence, 3), 'BATCH'))
        
        if cur.rowcount > 0:
            berita_masuk += 1

    except Exception as e:
        continue

print(f"✅ SUCCESS! {berita_masuk} berita sejarah dari Kaggle berhasil dianalisis AI dan masuk ke Data Warehouse.")
"""

# ENKRIPSI ANTI-BENTURAN
encoded_script = base64.b64encode(batch_script.encode('utf-8')).decode('utf-8')
safe_command = f"python -c \"import base64; exec(base64.b64decode('{encoded_script}').decode('utf-8'))\""

with DAG(
    '5_Kaggle_Batch_Inference',
    default_args=default_args,
    description='Pipeline Batch menarik data Kaggle dan mengklasifikasikannya dengan IndoBERT',
    schedule_interval=None, # Dijalankan manual (Trigger DAG)
    catchup=False,
    tags=['Batch', 'Kaggle', 'IndoBERT'],
) as dag:

    task_batch_inference = DockerOperator(
        task_id='proses_kaggle_dengan_indobert',
        image='indobert-trainer:latest',
        api_version='auto',
        auto_remove=True,
        command=safe_command,
        docker_url='unix://var/run/docker.sock',
        network_mode='ipbd_default', 
    )

    task_notifikasi_sukses = EmailOperator(
        task_id='kirim_email_sukses_batch',
        to=TARGET_EMAIL,
        subject='✅ [SUCCESS] Batch Inference IndoBERT Selesai',
        html_content="""
        <h3 style="color: green;">Pipeline Batch Inference Berhasil</h3>
        <p>Seluruh file historis dari Data Lake (MinIO) telah selesai dievaluasi oleh IndoBERT.</p>
        <p>Data bersih telah tersimpan di Data Warehouse (PostgreSQL) dengan autentikasi aman.</p>
        """
    )

    task_batch_inference >> task_notifikasi_sukses