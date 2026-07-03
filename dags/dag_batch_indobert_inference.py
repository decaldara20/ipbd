from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import base64

TARGET_EMAIL = 'kuntohidayat20@gmail.com'

default_args = {
    'owner': 'Jalu_dan_Kunto',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email': [TARGET_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

batch_script = """
import json
import tarfile
import psycopg2
import torch
import os
import re  # Modul tambahan untuk Data Protection (PII Masking)
from minio import Minio
from transformers import AutoTokenizer, AutoModelForSequenceClassification

print('🚀 [SISTEM] Memulai Batch Inference Pipeline dari Data Lake...')

# =========================================================================
# FUNGSI KEAMANAN: DATA PROTECTION & PII MASKING
# =========================================================================
def mask_pii(text):
    # 1. Sensor Alamat Email
    text = re.sub(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', '[EMAIL_TERSEMBUNYI]', text)
    # 2. Sensor Nomor Telepon (Format Indonesia/Internasional)
    text = re.sub(r'(?:\+62|62|0)[2-9]\d{7,11}', '[TELEPON_TERSEMBUNYI]', text)
    # 3. Sensor Nomor Identitas/KTP (16 Digit Angka)
    text = re.sub(r'\\b\d{16}\\b', '[NIK_TERSEMBUNYI]', text)
    return text

minio_client = Minio('minio:9000', access_key='admin', secret_key='password_minio_123', secure=False)

print("🧠 [ML ENGINE] Menarik model IndoBERT dari MinIO...")
minio_client.fget_object("macro-ekonomi-model", "indobert_sentiment_model.tar.gz", "indobert.tar.gz")

with tarfile.open("indobert.tar.gz", "r:gz") as tar:
    tar.extractall(path=".")

tokenizer = AutoTokenizer.from_pretrained("./final_model")
model = AutoModelForSequenceClassification.from_pretrained("./final_model")
model.eval()

print("🗄️ [DATABASE] Menghubungkan ke Data Warehouse...")
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow_secure_pass" 
}

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()
print("✅ [DATABASE] Autentikasi PostgreSQL berhasil.")

LABEL_MAP = {0: "Negatif", 1: "Netral", 2: "Positif"}

# =========================================================================
# FASE 2: DATA GOVERNANCE (DATA QUALITY PROFILING & AUDIT TRAIL)
# =========================================================================
dq_total_mentah = 0
dq_null_dibuang = 0
dq_error_tipe_data = 0
berita_masuk = 0

print("📂 [DATA] Memindai folder ekstraksi_batch/ di Data Lake...")
objects = minio_client.list_objects("macro-ekonomi-raw", prefix="ekstraksi_batch/", recursive=True)

for obj in objects:
    if not obj.object_name.endswith('.json'):
        continue
    
    response = minio_client.get_object("macro-ekonomi-raw", obj.object_name)
    data_batch = json.loads(response.read().decode('utf-8'))
    response.close()
    response.release_conn()
    
    for berita in data_batch:
        dq_total_mentah += 1
        
        judul = berita.get('judul', 'Tanpa Judul')
        teks_mentah = berita.get('isi_teks', '')
        waktu = berita.get('tanggal_publikasi', '2026-01-01 00:00:00')
        sumber = berita.get('sumber_media', 'Tidak Diketahui')
        
        # DATA QUALITY: Memfilter dan menghitung nilai Null/Kosong
        if not teks_mentah or teks_mentah.strip() == "":
            dq_null_dibuang += 1
            continue
            
        # EKSEKUSI DATA PROTECTION SEBELUM INFERENSI
        teks_aman = mask_pii(teks_mentah)
        
        inputs = tokenizer(teks_aman, return_tensors="pt", truncation=True, max_length=128)
        with torch.no_grad():
            outputs = model(**inputs)
            prob = torch.nn.functional.softmax(outputs.logits, dim=-1)
            prediksi = torch.argmax(prob, dim=-1).item()
            confidence = prob[0][prediksi].item()
        
        sentimen_final = LABEL_MAP[prediksi]
        
        t_lower = teks_aman.lower()
        if any(k in t_lower for k in ["ppn", "pajak"]): sub_isu = "Kebijakan Fiskal"
        elif any(k in t_lower for k in ["ihsg", "saham", "investasi"]): sub_isu = "Pasar Modal"
        elif any(k in t_lower for k in ["suku bunga", "bi", "rupiah"]): sub_isu = "Moneter"
        else: sub_isu = "Ekonomi Umum"
        
        try:
            # AUDIT TRAIL: Menyimpan identitas pengeksekusi dan memperbarui waktu_diubah
            cur.execute(\"""
                INSERT INTO berita_makroekonomi 
                (waktu_berita, sumber, judul_berita, sub_isu, sentimen, skor_kepercayaan, jalur_data, dieksekusi_oleh)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'Airflow_Batch_Pipeline')
                ON CONFLICT (judul_berita) DO UPDATE 
                SET waktu_berita = EXCLUDED.waktu_berita,
                    sumber = EXCLUDED.sumber,
                    sub_isu = EXCLUDED.sub_isu,
                    sentimen = EXCLUDED.sentimen,
                    skor_kepercayaan = EXCLUDED.skor_kepercayaan,
                    waktu_diubah = CURRENT_TIMESTAMP
            \""", (waktu, sumber, judul, sub_isu, sentimen_final, round(confidence, 3), 'BATCH'))
            
            if cur.rowcount > 0:
                berita_masuk += 1
                
        except Exception as e:
            # DATA QUALITY: Menghitung baris yang gagal akibat anomali database
            dq_error_tipe_data += 1
            continue

# MENCETAK LAPORAN DATA GOVERNANCE KE LOG AIRFLOW
print(f"\\n================ DATA GOVERNANCE REPORT ================")
print(f"Total Baris Mentah (Raw) : {dq_total_mentah}")
print(f"Data Quality (Null Teks) : {dq_null_dibuang} baris dibuang")
print(f"Data Quality (DB Error)  : {dq_error_tipe_data} baris gagal masuk")
print(f"Total Baris Valid & Bersih Masuk ke Data Warehouse: {berita_masuk}")
print(f"========================================================\\n")
print("✅ SUCCESS! Pipeline Batch dan Tata Kelola Data selesai.")
"""

encoded_script = base64.b64encode(batch_script.encode('utf-8')).decode('utf-8')
safe_command = f"python -c \"import base64; exec(base64.b64decode('{encoded_script}').decode('utf-8'))\""

with DAG(
    '2_Batch_IndoBERT_Inference',
    default_args=default_args,
    description='Membaca hasil Batch Ingestion, klasifikasi dengan IndoBERT, lalu simpan ke DB',
    schedule_interval='@daily',
    catchup=False,
    tags=['Batch', 'Inference', 'IndoBERT'],
) as dag:

    task_batch_inference = DockerOperator(
        task_id='proses_data_minio_dengan_indobert',
        image='indobert-trainer:latest',
        api_version='auto',
        auto_remove=True,
        command=safe_command,
        docker_url='unix://var/run/docker.sock',
        network_mode='ipbd_default', 
        mount_tmp_dir=False
    )

    task_notifikasi_sukses = EmailOperator(
        task_id='kirim_email_sukses_batch',
        to=TARGET_EMAIL,
        subject='✅ [SUCCESS] Batch Inference IndoBERT Selesai',
        html_content="""
        <h3 style="color: green;">Pipeline Batch Inference Berhasil</h3>
        <p>Seluruh file historis dari Data Lake (MinIO) telah selesai dievaluasi oleh IndoBERT.</p>
        <p>Data bersih telah tersimpan di Data Warehouse (PostgreSQL) dengan perlindungan PII dan pencatatan Audit Trail.</p>
        """
    )

    task_batch_inference >> task_notifikasi_sukses