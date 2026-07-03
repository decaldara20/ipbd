from kafka import KafkaConsumer
import psycopg2
import json
import os
import tarfile
from minio import Minio
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

print("⚙️ [SISTEM] Menginisialisasi koneksi infrastruktur Real-Time...")

# ==========================================
# 1. UNDUH & EKSTRAK MODEL DARI DATA LAKE
# ==========================================
print("🧠 [ML ENGINE] Menarik otak Deep Learning (IndoBERT) dari MinIO...")
minio_client = Minio(
    os.getenv('MINIO_HOST', 'minio:9000'),
    access_key=os.getenv('MINIO_USER', 'admin'),
    secret_key=os.getenv('MINIO_PASS', 'password_minio_123'),
    secure=False
)

model_tar_path = "indobert_sentiment_model.tar.gz"
extract_dir = "./final_model"

# Unduh hanya jika model belum ada di dalam memori kontainer
if not os.path.exists(extract_dir):
    minio_client.fget_object("macro-ekonomi-model", model_tar_path, model_tar_path)
    print("📦 [ML ENGINE] Mengekstrak arsitektur jaringan saraf tiruan...")
    with tarfile.open(model_tar_path, "r:gz") as tar:
        tar.extractall(path=".")
    os.remove(model_tar_path) # Bersihkan file tar untuk menghemat storage lokal

# ==========================================
# 2. MUAT MODEL KE RAM (Hanya dilakukan 1x di awal)
# ==========================================
tokenizer = AutoTokenizer.from_pretrained(extract_dir)
model = AutoModelForSequenceClassification.from_pretrained(extract_dir)
model.eval() # Kunci model ke mode Evaluasi (bukan mode Training)
print("✅ [ML ENGINE] IndoBERT siap mengeksekusi inferensi semantik!")

# ==========================================
# 3. KONEKSI DATA WAREHOUSE (POSTGRESQL)
# ==========================================
print("🗄️ [DATABASE] Menghubungkan ke Data Warehouse...")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()
print("✅ [DATABASE] Autentikasi PostgreSQL berhasil.")

# ==========================================
# 4. KAFKA LISTENER & INFERENSI REAL-TIME
# ==========================================
print("🗄️ [DATABASE] Memverifikasi skema Data Warehouse...")
query_buat_tabel = """
    CREATE TABLE IF NOT EXISTS berita_makroekonomi (
        id SERIAL PRIMARY KEY,
        waktu_sistem TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        waktu_berita TIMESTAMP NOT NULL,
        sumber VARCHAR(100) NOT NULL,
        judul_berita TEXT NOT NULL,
        sub_isu VARCHAR(50),
        sentimen VARCHAR(20),
        skor_kepercayaan NUMERIC(5,2),
        jalur_data VARCHAR(10)
    );
    CREATE INDEX IF NOT EXISTS idx_sub_isu ON berita_makroekonomi(sub_isu);
    DO $$ 
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unik_judul') THEN
            ALTER TABLE berita_makroekonomi ADD CONSTRAINT unik_judul UNIQUE (judul_berita);
        END IF;
    END $$;
"""
cur.execute(query_buat_tabel)
print("✅ [DATABASE] Skema tabel 'berita_makroekonomi' terverifikasi.")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
consumer = KafkaConsumer(
    'live-ekonomi',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("🎧 [CONSUMER] Siap mendengarkan aliran data agregator secara Real-Time...")

LABEL_MAP = {0: "Negatif", 1: "Netral", 2: "Positif"}

for message in consumer:
    data = message.value
    teks_berita = data.get('teks_berita', '')
    
    if not teks_berita: continue

# A. Inferensi AI IndoBERT
    inputs = tokenizer(teks_berita, return_tensors="pt", truncation=True, max_length=128)
    with torch.no_grad():
        outputs = model(**inputs)
        probabilitas = torch.nn.functional.softmax(outputs.logits, dim=-1)
        prediksi_angka = torch.argmax(probabilitas, dim=-1).item()
        
        # PERBAIKAN: Hapus (* 100). Biarkan tetap desimal (contoh: 0.985) agar muat di NUMERIC(4,3)
        confidence = probabilitas[0][prediksi_angka].item() 

    sentimen_final = LABEL_MAP[prediksi_angka]

    # B. Kategorisasi Sub Isu (Rule-Based)
    teks_lower = teks_berita.lower()
    if any(k in teks_lower for k in ["ppn", "pajak"]): sub_isu = "Kebijakan Fiskal"
    elif any(k in teks_lower for k in ["ihsg", "saham", "investasi"]): sub_isu = "Pasar Modal"
    elif any(k in teks_lower for k in ["suku bunga", "bi", "rupiah"]): sub_isu = "Moneter"
    else: sub_isu = "Ekonomi Umum"
    
    # C. Simpan ke PostgreSQL
    try:
        cur.execute("""
            INSERT INTO berita_makroekonomi 
            (waktu_berita, sumber, judul_berita, sub_isu, sentimen, skor_kepercayaan, jalur_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (judul_berita) DO NOTHING
        """, (
            data.get('waktu_publikasi'), 
            data.get('sumber'), 
            teks_berita, 
            sub_isu,
            sentimen_final,
            round(confidence, 3), # Dibulatkan ke 3 angka di belakang koma
            'STREAM'
        ))
        
        if cur.rowcount > 0:
            ikon = "🔴" if sentimen_final == "Negatif" else "🟢" if sentimen_final == "Positif" else "⚪"
            # Dikali 100 hanya untuk tampilan di terminal agar enak dibaca (misal 98.5%)
            print(f"[{data.get('sumber')}] {ikon} {sentimen_final} ({confidence*100:.1f}%) | Isu: {sub_isu} -> {teks_berita[:50]}...")
            
    except Exception as e:
        print(f"⚠️ Kegagalan Database: {e}")