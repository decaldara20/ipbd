from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import base64

TARGET_EMAIL = 'kuntohidayat20@gmail.com'

default_args = {
    'owner': 'Jalu_dan_Kunto',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 29),
    'email': [TARGET_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0, 
    'retry_delay': timedelta(minutes=10),
}

# =========================================================================
# SCRIPT DEEP LEARNING (Revisi Logika Preprocessing)
# =========================================================================
training_script = """
import io
import json
import torch
import pandas as pd
import os
from minio import Minio
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import Trainer, TrainingArguments
from datasets import Dataset

print('🚀 Memulai Inisiasi Deep Learning IndoBERT Environment...')

minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password_minio_123",
    secure=False
)

print('📥 Menyedot Dataset Ground Truth dari Data Lake...')
response = minio_client.get_object("macro-ekonomi-raw", "training_data/indonesian_financial_sentiment_gt.json")
raw_data = json.loads(response.read().decode('utf-8'))
response.close()
response.release_conn()

df = pd.DataFrame(raw_data)

# PERBAIKAN LOGIKA DATA: 
# Dataset HuggingFace ini sudah berformat angka (0=Negatif, 1=Netral, 2=Positif).
df = df.dropna(subset=['label_asli', 'teks'])
df['label'] = df['label_asli'].astype(int)

# Sisakan hanya kolom yang dibutuhkan oleh HuggingFace agar memori efisien
df = df[['teks', 'label']]

hf_dataset = Dataset.from_pandas(df)
dataset_split = hf_dataset.train_test_split(test_size=0.2, seed=42)

model_name = "indobenchmark/indobert-base-p1"
print(f'🧠 Mengunduh arsitektur model {model_name} (sekitar 400MB)...')

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=3)

def tokenize_function(examples):
    return tokenizer(examples["teks"], padding="max_length", truncation=True, max_length=128)

tokenized_datasets = dataset_split.map(tokenize_function, batched=True)

training_args = TrainingArguments(
    output_dir="./results",
    eval_strategy="epoch",
    learning_rate=2e-5,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    num_train_epochs=3,
    weight_decay=0.01,
    use_cpu=True,
    logging_steps=10
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_datasets["train"],
    eval_dataset=tokenized_datasets["test"],
)

print('🔥 Memulai Proses Pelatihan Deep Learning (Epochs: 3)...')
trainer.train()

print('💾 Menyimpan model ke memori sementara...')
model.save_pretrained("./final_model")
tokenizer.save_pretrained("./final_model")

print('☁️ Mengunggah Model Weights dan Tokenizer ke MinIO...')
import tarfile

with tarfile.open("indobert_sentiment_model.tar.gz", "w:gz") as tar:
    tar.add("./final_model", arcname="final_model")

file_size = os.path.getsize("indobert_sentiment_model.tar.gz")
with open("indobert_sentiment_model.tar.gz", "rb") as file_data:
    minio_client.put_object(
        "macro-ekonomi-model", 
        "indobert_sentiment_model.tar.gz", 
        file_data, 
        length=file_size
    )

print('✅ SUCCESS! Pipeline Deep Learning Selesai. Model siap disajikan.')
"""

encoded_script = base64.b64encode(training_script.encode('utf-8')).decode('utf-8')
safe_command = f"python -c \"import base64; exec(base64.b64decode('{encoded_script}').decode('utf-8'))\""

# =========================================================================
# DEFINISI DAG AIRFLOW
# =========================================================================
with DAG(
    '4_IndoBERT_FineTuning_Pipeline',
    default_args=default_args,
    description='Melatih model Deep Learning IndoBERT di kontainer terisolasi',
    schedule_interval='@monthly', 
    catchup=False,
    tags=['Deep_Learning', 'IndoBERT', 'Training', 'Docker'],
) as dag:

    task_train_indobert = DockerOperator(
        task_id='fine_tune_indobert_model',
        image='indobert-trainer:latest',
        api_version='auto',
        auto_remove=True,
        command=safe_command,
        docker_url='unix://var/run/docker.sock',
        network_mode='ipbd_default', 
        mount_tmp_dir=False,
        environment={
            'TRANSFORMERS_CACHE': '/tmp', 
        }
    )

    task_notifikasi_sukses = EmailOperator(
        task_id='kirim_email_sukses_training',
        to=TARGET_EMAIL,
        subject='✅ [SUCCESS] Model IndoBERT Selesai Dilatih',
        html_content="""
        <h3 style="color: green;">Pipeline Deep Learning Berhasil</h3>
        <p>Kontainer terisolasi telah berhasil melatih model IndoBERT menggunakan dataset finansial.</p>
        <p>Artifacts model <strong>(indobert_sentiment_model.tar.gz)</strong> telah aman tersimpan di MinIO (macro-ekonomi-model).</p>
        """
    )

    task_train_indobert >> task_notifikasi_sukses