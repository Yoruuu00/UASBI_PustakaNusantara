import pandas as pd
import requests
import time
from datetime import datetime
import os
import json
import numpy as np

# ============================================================================
# KONFIGURASI
# ============================================================================

if os.path.exists('/app'):
    BASE_PATH = '/app'
else:
    BASE_PATH = '.'

DATA_PATH = os.path.join(BASE_PATH, 'data')
OUTPUT_PATH = os.path.join(BASE_PATH, 'output')
LOG_PATH = os.path.join(BASE_PATH, 'logs')

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# ============================================================================
# LOGGING
# ============================================================================

def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_text = f"[{timestamp}] [{level}] {message}"
    print(log_text)
    
    log_file = os.path.join(LOG_PATH, f"api_enrichment_{datetime.now().strftime('%Y%m%d')}.txt")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_text + '\n')

# ============================================================================
# OPEN LIBRARY API FUNCTIONS
# ============================================================================

def get_default_data(found=False):
    """Mengembalikan data default agar tidak ada missing values"""
    return {
        'api_found': found,
        'api_title': 'Unknown',
        'api_author': 'Unknown Author',
        'api_authors_all': 'Unknown Author',
        'api_first_publish_year': 2020, # Default tahun
        'api_isbn': 0,
        'api_isbn_all': '0',
        'api_publisher': 'Unknown Publisher',
        'api_publishers_all': 'Unknown Publisher',
        'api_language': 'en',
        'api_number_of_pages': 200, # Default halaman
        'api_subject': 'General',
        'api_edition_count': 1,
        'api_has_fulltext': False,
        'api_search_url': 'https://openlibrary.org/'
    }

def search_book_by_title(title, max_retries=3):
    """
    Search book by title using Open Library Search API
    """
    base_url = "https://openlibrary.org/search.json"
    
    # Bersihkan judul
    clean_title = str(title).strip()
    if not clean_title or clean_title.lower() == 'nan':
         return get_default_data(found=False)

    for attempt in range(max_retries):
        try:
            params = {
                'title': clean_title,
                'limit': 1
            }
            
            response = requests.get(base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('docs') and len(data['docs']) > 0:
                    book = data['docs'][0]
                    
                    # Ambil data REAL jika ada, jika tidak pakai Default
                    return {
                        'api_found': True,
                        'api_title': book.get('title', 'Unknown'),
                        'api_author': book.get('author_name', ['Unknown Author'])[0],
                        'api_authors_all': ', '.join(book.get('author_name', [])) if book.get('author_name') else 'Unknown',
                        'api_first_publish_year': book.get('first_publish_year', 2020),
                        'api_isbn': book.get('isbn', [0])[0],
                        'api_isbn_all': ', '.join(book.get('isbn', [])[:3]) if book.get('isbn') else '0',
                        'api_publisher': book.get('publisher', ['Unknown'])[0],
                        'api_publishers_all': ', '.join(book.get('publisher', [])[:3]) if book.get('publisher') else 'Unknown',
                        'api_language': book.get('language', ['en'])[0],
                        'api_number_of_pages': book.get('number_of_pages_median', 200),
                        'api_subject': ', '.join(book.get('subject', [])[:5]) if book.get('subject') else 'General',
                        'api_edition_count': book.get('edition_count', 1),
                        'api_has_fulltext': book.get('has_fulltext', False),
                        'api_search_url': f"https://openlibrary.org{book.get('key', '')}" if book.get('key') else 'https://openlibrary.org/'
                    }
                else:
                    # API Sukses tapi buku tidak ketemu -> Pakai Default
                    return get_default_data(found=False)
                    
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2)
            
        except Exception as e:
            log_message(f"⚠ Error '{title[:20]}': {e}", "WARNING")
            
    # Jika gagal total (koneksi error) -> Pakai Default
    return get_default_data(found=False)

def enrich_dataset_with_api(df, title_column='Title', sample_size=None, delay=0.5):
    log_message("="*70)
    log_message("OPEN LIBRARY API ENRICHMENT")
    log_message("="*70)
    
    if title_column not in df.columns:
        log_message(f"❌ Kolom '{title_column}' tidak ditemukan!", "ERROR")
        return df
    
    # Get unique titles
    unique_titles = df[title_column].dropna().unique()
    total_unique = len(unique_titles)
    
    # SETTING PENTING: Jika sample_size None, proses semua.
    # Jika sample_size ada angkanya, ambil sampel saja.
    if sample_size and sample_size < total_unique:
        titles_to_process = np.random.choice(unique_titles, sample_size, replace=False)
        log_message(f"📌 Processing sample: {sample_size} books")
    else:
        titles_to_process = unique_titles
        log_message(f"📌 Processing ALL books: {total_unique} books")
    
    # Estimate time
    estimated_time = (len(titles_to_process) * (delay + 0.5)) / 60
    log_message(f"⏱️ Estimated time: {estimated_time:.1f} minutes")
    
    enriched_data = []
    
    for idx, title in enumerate(titles_to_process, 1):
        # Progress log simpel biar gak spam
        if idx % 5 == 0 or idx == 1 or idx == len(titles_to_process):
             log_message(f"[{idx}/{len(titles_to_process)}] Processing: {str(title)[:30]}...")
        
        api_result = search_book_by_title(title)
        api_result['original_title'] = title
        
        enriched_data.append(api_result)
        time.sleep(delay)
    
    # Convert to DataFrame
    enriched_df = pd.DataFrame(enriched_data)
    
    # Merge dengan dataset asli
    log_message("Merging Data...")
    result_df = df.merge(
        enriched_df,
        left_on=title_column,
        right_on='original_title',
        how='left',
        suffixes=('', '_api_dup')
    )
    
    if 'original_title' in result_df.columns:
        result_df.drop('original_title', axis=1, inplace=True)
    
    # Final check untuk mengisi sisa-sisa NaN (jika ada judul yang tidak terproses karena bukan unique)
    log_message("Final Filling for Completeness...")
    default_vals = get_default_data(found=False)
    for col, val in default_vals.items():
        if col in result_df.columns:
            result_df[col] = result_df[col].fillna(val)

    return result_df

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    log_message("")
    log_message("="*70)
    log_message(" OPEN LIBRARY API ENRICHMENT PIPELINE")
    log_message(" Toko Buku Pustaka Nusantara")
    log_message("="*70)
    
    # Cek file input
    # Cari di folder data/ dulu, lalu root
    input_file = 'data/Merged_OLTP_Books_Cleaned0.csv'
    if not os.path.exists(input_file):
         input_file = 'Merged_OLTP_Books_Cleaned0.csv'
    
    if not os.path.exists(input_file):
        log_message(f"❌ File data tidak ditemukan!", "ERROR")
        return None
    
    log_message(f"📂 Loading raw data from: {input_file}")
    df = pd.read_csv(input_file)
    
    # CONFIGURATION
    # Set SAMPLE_SIZE = None agar memproses SEMUA data
    # Set DELAY = 0.5 agar agak cepat tapi aman
    SAMPLE_SIZE = None 
    DELAY_SECONDS = 0.2
    
    # Enrich
    enriched_df = enrich_dataset_with_api(
        df, 
        title_column='Title',
        sample_size=SAMPLE_SIZE,
        delay=DELAY_SECONDS
    )
    
    if enriched_df is None:
        return None
    
    # Save enriched dataset
    output_file = os.path.join(OUTPUT_PATH, 'ML_Dataset_Enriched.csv')
    enriched_df.to_csv(output_file, index=False)
    
    log_message(f"✓ Selesai! Data tersimpan di: {output_file}")
    log_message(f"✓ Completeness dijamin 100% karena menggunakan default values.")
    
    return enriched_df

if __name__ == "__main__":
    main()