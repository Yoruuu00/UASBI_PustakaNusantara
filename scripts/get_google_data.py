import pandas as pd
import requests
import time
import json
import os
from tqdm import tqdm  # Library untuk progress bar

# ============================================================================
# KONFIGURASI
# ============================================================================
INPUT_FILE = 'output/ML_Dataset_From_DB.csv'
OUTPUT_FILE = 'output/ML_Dataset_Enriched_Google.csv'
CACHE_FILE = 'google_books_cache.json'

def get_book_info_from_google(isbn, title, author):
    """
    Mencari data buku di Google Books API.
    Prioritas: ISBN -> Title + Author -> Title Only
    """
    base_url = "https://www.googleapis.com/books/v1/volumes"
    
    # 1. Coba cari pakai ISBN (Paling Akurat)
    if isbn and str(isbn).lower() != 'nan' and str(isbn).lower() != 'unknown':
        clean_isbn = str(isbn).replace('-', '').strip()
        try:
            response = requests.get(f"{base_url}?q=isbn:{clean_isbn}")
            if response.status_code == 200:
                data = response.json()
                if 'items' in data:
                    return data['items'][0]['volumeInfo']
        except:
            pass

    # 2. Coba cari pakai Judul + Penulis (Jika ISBN gagal)
    if title and str(title).lower() != 'unknown':
        query = f"intitle:{title}"
        if author and str(author).lower() != 'unknown':
            query += f"+inauthor:{author}"
            
        try:
            response = requests.get(f"{base_url}?q={query}&maxResults=1")
            if response.status_code == 200:
                data = response.json()
                if 'items' in data:
                    return data['items'][0]['volumeInfo']
        except:
            pass

    return None

def main():
    print("🚀 MEMULAI PROSES DATA ENRICHMENT (GOOGLE BOOKS API)...")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"📖 Membaca {len(df)} baris data...")
    except FileNotFoundError:
        print("❌ File input tidak ditemukan!")
        return

    # 2. Load Cache (Agar tidak request ulang yang sudah sukses)
    cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
        print(f"📦 Cache dimuat: {len(cache)} buku sudah tersimpan.")

    # 3. Kolom Baru yang akan diisi
    new_data = {
        'google_rating': [],
        'google_ratings_count': [],
        'google_page_count': [],
        'google_categories': [],
        'google_description': []
    }

    # 4. Loop setiap baris data (Pakai tqdm biar ada loading bar)
    # Kita install tqdm dulu kalau belum ada: pip install tqdm
    print("⏳ Sedang menghubungi Google... (Mohon bersabar)")
    
    rows_processed = 0
    
    for index, row in tqdm(df.iterrows(), total=len(df)):
        # Kunci unik untuk cache: Judul
        title_key = str(row.get('Title', 'Unknown')).strip()
        
        # Cek Cache dulu
        if title_key in cache:
            info = cache[title_key]
        else:
            # Kalau tidak ada di cache, Panggil API
            isbn = row.get('api_isbn', '')
            author = row.get('api_author', '')
            
            info_raw = get_book_info_from_google(isbn, title_key, author)
            
            # Rapikan hasil
            if info_raw:
                info = {
                    'rating': info_raw.get('averageRating', 0),
                    'count': info_raw.get('ratingsCount', 0),
                    'pages': info_raw.get('pageCount', 0),
                    'categories': ", ".join(info_raw.get('categories', ['Unknown'])),
                    'desc': info_raw.get('description', '')
                }
            else:
                # Kalau tidak ketemu, isi default
                info = {'rating': 0, 'count': 0, 'pages': 0, 'categories': 'Unknown', 'desc': ''}
            
            # Simpan ke cache
            cache[title_key] = info
            
            # Simpan cache ke file setiap 50 request (Biar aman kalau crash)
            if rows_processed % 50 == 0:
                with open(CACHE_FILE, 'w') as f:
                    json.dump(cache, f)
            
            # Tidur sebentar biar tidak diblokir Google (Rate Limiting)
            time.sleep(0.5) 

        # Masukkan ke list sementara
        new_data['google_rating'].append(info['rating'])
        new_data['google_ratings_count'].append(info['count'])
        new_data['google_page_count'].append(info['pages'])
        new_data['google_categories'].append(info['categories'])
        new_data['google_description'].append(info['desc'])
        
        rows_processed += 1

    # Simpan cache terakhir
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

    # 5. Gabungkan ke DataFrame
    for col, values in new_data.items():
        df[col] = values

    # 6. Simpan File Baru
    df.to_csv(OUTPUT_FILE, index=False)
    print("\n" + "="*50)
    print("✅ SELESAI! Data berhasil diperkaya.")
    print(f"📂 File baru: {OUTPUT_FILE}")
    print("="*50)

if __name__ == "__main__":
    main()