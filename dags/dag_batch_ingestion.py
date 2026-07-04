from airflow import DAG 
import requests
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta 
import feedparser 
from newspaper import Article, Config 
from minio import Minio 
import json 
import time 
import io 
import urllib.parse 
import os 
from airflow.operators.email import EmailOperator

# ========================================== 
# KONFIGURASI KEAMANAN & ALERTING 
# ========================================== 
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

def ekstraksi_berita_ke_minio(**kwargs): 
    minio_client = Minio( 
        "minio:9000", 
        access_key=MINIO_USER, 
        secret_key=MINIO_PASS, 
        secure=False 
    ) 
    if not minio_client.bucket_exists("macro-ekonomi-raw"): 
        minio_client.make_bucket("macro-ekonomi-raw") 

    config = Config() 
    config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36' 
    config.request_timeout = 15 

    # STRATEGI: Perluas kata kunci makroekonomi ke sektor komoditas, finansial, dan kebijakan global
    kata_kunci = [
        "IHSG", "inflasi Indonesia", "suku bunga BI", "pajak PPN", "PHK startup", "daya beli masyarakat",
        "pertumbuhan ekonomi RI", "nilai tukar rupiah", "investasi asing", "kebijakan fiskal", "ekspor impor",
        # Tambahan kata kunci baru untuk memperbanyak volume data:
        "harga emas ANTAM", "subsidi BBM", "neraca perdagangan", "utang luar negeri", "bursa efek indonesia",
        "stimulus ekonomi", "crypto indonesia", "UMKM naik kelas", "tarif cukai", "APBN 2026"
    ] 
    
    semua_berita = [] 
    url_terproses = set() # Untuk mencegah duplikasi antar RSS maupun NewsAPI

    # ==========================================
    # BAGIAN 1: PROSES AMBIL DATA DARI RSS FEED
    # ==========================================
    for keyword in kata_kunci: 
        query_url = urllib.parse.quote(keyword) 
        
        urls_rss = [
            f"https://www.bing.com/news/search?q={query_url}&format=rss&cc=id&setlang=id",
            f"https://news.google.com/rss/search?q={query_url}&hl=id&gl=ID&ceid=ID:id"
        ]
        
        for rss_url in urls_rss:
            feed = feedparser.parse(rss_url) 
            for entry in feed.entries[:20]: 
                url_asli = entry.link 
                judul = entry.title 
                
                # Cek duplikasi URL
                if url_asli in url_terproses:
                    continue
                
                try: 
                    if "bloomberg" in url_asli: 
                        continue 
                        
                    news_article = Article(url_asli, language='id', config=config) 
                    news_article.download() 
                    news_article.parse() 
                    
                    if news_article.text and len(news_article.text.strip()) > 100: 
                        # Membersihkan teks media dari domain atau membersihkan awalan "STREAM-"
                        raw_source = urllib.parse.urlparse(url_asli).netloc
                        clean_source = raw_source.replace("STREAM-", "")
                        
                        semua_berita.append({ 
                            "keyword_pencarian": keyword, 
                            "judul": judul, 
                            "tanggal_publikasi": entry.published if hasattr(entry, 'published') else time.strftime("%Y-%m-%d"), 
                            "sumber_media": clean_source, 
                            "url_asli": url_asli, 
                            "isi_teks": news_article.text[:1000] 
                        }) 
                        url_terproses.add(url_asli) # Tandai URL sudah sukses diambil
                        
                        # Jeda sedikit diperkecil agar eksekusi data yang melimpah tidak terlalu lama
                        time.sleep(1.0) 
                except Exception as e: 
                    print(f"Error pada {url_asli}: {e}") 

    API_KEY = "eec8ddc25ab3e781070a30ebb5ff2257"
    
    query_string = " OR ".join([f'"{kw}"' for kw in kata_kunci])
    news_api_url = f"https://newsapi.org{urllib.parse.quote(query_string)}&language=id&sortBy=publishedAt&pageSize=100&apiKey={API_KEY}"
    
    try:
        import requests
        response = requests.get(news_api_url, timeout=15)
        if response.status_code == 200:
            data_json = response.json()
            for art in data_json.get("articles", []):
                url_api = art.get("url")
                
                if not url_api or url_api in url_terproses:
                    continue
                if art.get("title") == "[Removed]":
                    continue
                
                raw_source = art.get("source", {}).get("name", "Unknown Source")
                clean_source = raw_source.replace("STREAM-", "") if raw_source else "Unknown Source"
                
                semua_berita.append({
                    "keyword_pencarian": "Ekonomi Makro (NewsAPI)",
                    "judul": art.get("title"),
                    "tanggal_publikasi": art.get("publishedAt"),
                    "sumber_media": clean_source,
                    "url_asli": url_api,
                    "isi_teks": art.get("description") if art.get("description") else art.get("title")
                })
                url_terproses.add(url_api)
        else:
            print(f"Gagal mengambil data NewsAPI. Status: {response.status_code}")
    except Exception as e:
        print(f"Error pada integrasi NewsAPI: {e}")

    # ==========================================
    # BAGIAN 3: SIMPAN DATA MASAL KE MINIO
    # ==========================================
    if semua_berita: 
        json_data = json.dumps(semua_berita, indent=4, ensure_ascii=False) 
        data_bytes = json_data.encode('utf-8') 
        nama_file = f"ekstraksi_batch/berita_historis_harian_{datetime.now().strftime('%Y%m%d')}.json" 
        minio_client.put_object("macro-ekonomi-raw", nama_file, io.BytesIO(data_bytes), len(data_bytes), "application/json") 
        return f"Sukses mengekstrak total {len(semua_berita)} berita (RSS + NewsAPI)." 

    raise ValueError("DATA QUALITY FAILURE: Tidak ada berita yang berhasil ditarik. Memicu notifikasi email...")  

with DAG( 
    '1_Daily_Batch_Ingestion', 
    default_args=default_args, 
    description='Mengekstrak berita ekonomi harian ke Data Lake', 
    schedule_interval='@daily', 
    catchup=False, 
    tags=['Ingestion', 'Batch', 'MinIO'], 
) as dag: 

    task_ekstraksi = PythonOperator( 
        task_id='tarik_berita_multi_source', 
        python_callable=ekstraksi_berita_ke_minio, 
    ) 

    task_notifikasi_sukses = EmailOperator( 
        task_id='kirim_email_sukses', 
        to=TARGET_EMAIL, 
        subject='[SUCCESS] Ekstraksi Berita Harian Selesai', 
        html_content=""" 
        <h3 style="color: green;">Data Pipeline Sukses</h3> 
        <p>Proses ekstraksi berita ekonomi dari Bing & Google News ke Data Lake (MinIO) telah berhasil dieksekusi secara otomatis.</p> 
        """ 
    ) 

    task_ekstraksi >> task_notifikasi_sukses