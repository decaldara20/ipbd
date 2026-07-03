from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from minio import Minio
import requests
import json
import pandas as pd
import io
import os

TARGET_EMAIL = 'kuntohidayat20@gmail.com'
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'admin')
MINIO_PASS = os.getenv('MINIO_ROOT_PASSWORD', 'password_minio_123')

default_args = {
    'owner': 'Jalu_dan_Kunto',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 29),
    'email': [TARGET_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def unduh_dan_simpan_dataset_hf(**kwargs):
    """Menarik data finansial berlabel dari HuggingFace API dengan metode Pagination"""
    minio_client = Minio(
        "minio:9000",
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )
    
    if not minio_client.bucket_exists("macro-ekonomi-raw"):
        minio_client.make_bucket("macro-ekonomi-raw")

    print("📡 Menghubungi HuggingFace Datasets Server API dengan Pagination...")
    
    ekstrak_fitur = []
    target_total_data = 500
    limit_per_request = 100 # Batas maksimal yang diizinkan HuggingFace
    
    # Looping Pagination: 0, 100, 200, 300, 400
    for offset in range(0, target_total_data, limit_per_request):
        url_hf = f"https://datasets-server.huggingface.co/rows?dataset=intanm%2Findonesian-financial-sentiment-analysis&config=default&split=train&offset={offset}&length={limit_per_request}"
        
        response = requests.get(url_hf, timeout=30)
        
        # Jika API menolak, hentikan loop dan gunakan data yang sudah berhasil ditarik sejauh ini
        if response.status_code != 200:
            print(f"⚠️ API berhenti di offset {offset}. Status Code: {response.status_code}")
            break
            
        raw_json = response.json()
        rows_data = raw_json.get('rows', [])
        
        if not rows_data:
            break # Hentikan jika data sudah habis
            
        for row in rows_data:
            konten_baris = row.get('row', {})
            ekstrak_fitur.append({
                "teks": konten_baris.get('text', ''),
                "label_asli": konten_baris.get('label', '')
            })
            
        print(f"✅ Berhasil menarik data dari offset {offset} ke {offset + limit_per_request}")

    if not ekstrak_fitur:
        raise ValueError("DATA QUALITY FAILURE: Dataset HuggingFace gagal ditarik sepenuhnya.")

    df_training = pd.DataFrame(ekstrak_fitur)
    print(f"📊 Total akhir data latih terkumpul: {len(df_training)} baris.")

    # Konversi ke format JSON bytes
    json_data = df_training.to_json(orient='records', force_ascii=False, indent=4)
    data_bytes = json_data.encode('utf-8')
    nama_file_minio = "training_data/indonesian_financial_sentiment_gt.json"
    
    minio_client.put_object(
        "macro-ekonomi-raw",
        nama_file_minio,
        io.BytesIO(data_bytes),
        len(data_bytes),
        "application/json"
    )
    print(f"💾 File berhasil diamankan di MinIO: macro-ekonomi-raw/{nama_file_minio}")
    return f"Infrastruktur Data Terpusat: {len(df_training)} data latih siap digunakan."

with DAG(
    '3_Ingest_Training_Data',
    default_args=default_args,
    description='Mengunduh Dataset Finansial Berlabel dari HuggingFace ke MinIO Data Lake',
    schedule_interval='@once', # Hanya perlu dijalankan sekali untuk mengunci data latih historis
    catchup=False,
    tags=['Deep_Learning', 'Ingestion', 'HuggingFace', 'MinIO']
) as dag:

    task_ingest_hf = PythonOperator(
        task_id='fetch_huggingface_dataset',
        python_callable=unduh_dan_simpan_dataset_hf,
    )

    task_notifikasi_sukses = EmailOperator(
        task_id='kirim_email_sukses_ingest',
        to=TARGET_EMAIL,
        subject='✅ [SUCCESS] Ingesti Data Latih Deep Learning Selesai',
        html_content="""
        <h3 style="color: green;">Langkah 1 Selesai Secara Mutlak</h3>
        <p>Dataset finansial berlabel asli dari HuggingFace berhasil ditarik dan disimpan ke dalam MinIO Object Storage.</p>
        <p>Fondasi data untuk proses fine-tuning model IndoBERT telah terkunci dan aman.</p>
        """
    )

    task_ingest_hf >> task_notifikasi_sukses