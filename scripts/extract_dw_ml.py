import pandas as pd
import mysql.connector
import os
import warnings

# Matikan warning SQLAlchemy biar terminal bersih
warnings.filterwarnings('ignore')

# ============================================================================
# KONFIGURASI DATABASE
# ============================================================================
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'pustaka_dw' 

OUTPUT_FILE = 'output/ML_Dataset_From_DB.csv'

def extract_data():
    print("\n" + "="*60)
    print("🔌 ETL STEP 1: EXTRACT FROM DATA WAREHOUSE (MySQL)")
    print("="*60)
    
    connection = None
    try:
        # 1. Koneksi Database
        print(f"   Menghubungkan ke database '{DB_NAME}'...")
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        
        if connection.is_connected():
            print("   ✅ BERHASIL TERHUBUNG!")
            
            # 2. Query SQL (FINAL FIX)
            # - `Item Price`  : Pakai Backtick (Karena data asli pakai spasi)
            # - Total_Amount  : Pakai Underscore (Karena hasil olahan Python)
            
            print("   ⏳ Sedang menjalankan Query SQL...")
            
            query = """
            SELECT 
                f.`Purchase-date` AS Purchase_date,
                f.`Customer ID` AS Customer_ID,
                f.Title,
                f.`Item Price` AS Item_Price,     -- DATA ASLI (SPASI)
                f.Total_Amount,                   -- HASIL PYTHON (UNDERSCORE)
                f.Quantity,
                f.Category_OLTP AS Category,
                b.Book_Age,
                b.Author
            FROM fact_sales f
            LEFT JOIN dimbook b ON f.Title = b.Title
            """
            
            # 3. Load ke Pandas
            df = pd.read_sql(query, connection)
            
            # 4. Validasi & Simpan
            if df.empty:
                print("   ❌ WARNING: Data kosong! Cek isi tabel fact_sales.")
            else:
                print(f"   📊 Data berhasil ditarik: {len(df)} baris.")
                print(f"   🔍 Contoh Judul: {df['Title'].iloc[0]}")
                print(f"   💰 Contoh Total: {df['Total_Amount'].iloc[0]}")
                
                os.makedirs('output', exist_ok=True)
                df.to_csv(OUTPUT_FILE, index=False)
                print(f"   💾 Data disimpan ke: {OUTPUT_FILE}")
                
                print("\n" + "="*60)
                print("🚀 STATUS: SUKSES! LANJUT KE STEP 2.")
                print("   Jalankan: python scripts/get_google_data.py")
                print("="*60)

    except mysql.connector.Error as err:
        print(f"\n❌ DATABASE ERROR: {err}")
        print("   Sepertinya struktur tabel Anda unik (campuran spasi dan underscore).")
        print("   Silakan cek HeidiSQL dan sesuaikan nama kolom di script ini jika masih error.")

    except Exception as e:
        print(f"\n❌ SYSTEM ERROR: {e}")

    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\n🔒 Koneksi database ditutup.")

if __name__ == "__main__":
    extract_data()