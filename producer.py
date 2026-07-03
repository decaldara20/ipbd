from kafka import KafkaProducer
import feedparser
import urllib.parse
import json
import time
import uuid
import os
import random
import re

# ==========================================
# KONFIGURASI KAFKA
# ==========================================
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# ==========================================
# SUMBER DATA RIIL (GOOGLE NEWS AGGREGATOR)
# ==========================================
# Topik ini akan memastikan data yang ditarik tetap relevan dengan konteks proyek
TOPIK_PENCARIAN = [
    "ekonomi makro indonesia", 
    "suku bunga BI", 
    "inflasi indonesia", 
    "IHSG bursa efek", 
    "kebijakan fiskal pajak"
]

print(f"🌍 [PRODUCER] Memulai streaming agregasi berita ekonomi RIIL ke {KAFKA_BROKER}...")

berita_terkirim = set()

def hapus_tag_html(teks):
    if not teks: return ""
    return re.sub(r'<.*?>', '', teks).strip()

try:
    while True:
        ada_berita_baru = False
        
        # Acak urutan topik agar stream tidak monoton
        random.shuffle(TOPIK_PENCARIAN)
        
        for topik in TOPIK_PENCARIAN:
            try:
                # Mengubah teks menjadi format URL (URL Encoding)
                query_url = urllib.parse.quote(topik)
                # URL RSS Google News berbahasa Indonesia
                url = f"https://news.google.com/rss/search?q={query_url}&hl=id&gl=ID&ceid=ID:id"
                
                feed = feedparser.parse(url)
                
                # Batasi hanya mengambil 5 berita teratas per topik untuk menjaga sifat 'Real-Time'
                for entry in feed.entries[:5]:
                    if entry.link not in berita_terkirim:
                        judul = entry.title
                        ringkasan = hapus_tag_html(entry.get('summary', ''))
                        teks_gabungan = f"{judul}. {ringkasan}"
                        
                        # Ekstrak nama sumber media asli dari Google News (contoh: "Kompas.com", "Detik Finance")
                        sumber_media = entry.source.title if hasattr(entry, 'source') else 'Media-Nasional-Agregat'
                        
                        # Filter opsional: Abaikan jika ringkasannya kosong atau terlalu pendek
                        if len(teks_gabungan) < 30: continue
                        
                        data = {
                            "id_pesan": str(uuid.uuid4()),
                            "sumber": f"{sumber_media.upper().replace(' ', '')}",
                            "waktu_publikasi": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "teks_berita": teks_gabungan,
                            "url_asli": entry.link
                        }
                        
                        producer.send('live-ekonomi', value=data)
                        print(f"[{time.strftime('%H:%M:%S')}] 📡 [{sumber_media}] -> {judul[:50]}...")
                        
                        berita_terkirim.add(entry.link)
                        ada_berita_baru = True
                        
                        # Delay natural stream
                        time.sleep(random.uniform(1.5, 3.5))
                        
            except Exception as e:
                print(f"⚠️ Gagal menarik agregat untuk topik '{topik}': {e}")
        
        if not ada_berita_baru:
            print(f"[{time.strftime('%H:%M:%S')}] ⏳ Memantau... Tidak ada berita baru. Siklus jeda 2 menit...")
            time.sleep(120) # Jeda lebih lama jika tidak ada berita baru
        else:
            # Jeda antar siklus sapuan penuh
            time.sleep(30)

except KeyboardInterrupt:
    print("\n🛑 Streaming dihentikan secara manual.")
finally:
    producer.close()