import pandas as pd

# Load data baru
df = pd.read_csv('output/ML_Dataset_Enriched_Google.csv')

print("📊 LAPORAN KUALITAS DATA GOOGLE BOOKS")
print("="*50)
print(f"Total Baris Data: {len(df)}")

# Cek Rating
with_rating = df[df['google_rating'] > 0]
print(f"✅ Punya Rating: {len(with_rating)} buku ({len(with_rating)/len(df)*100:.1f}%)")

# Cek Halaman
with_pages = df[df['google_page_count'] > 0]
print(f"✅ Punya Info Halaman: {len(with_pages)} buku ({len(with_pages)/len(df)*100:.1f}%)")

# Cek Deskripsi
with_desc = df[df['google_description'].notna() & (df['google_description'] != '')]
print(f"✅ Punya Deskripsi: {len(with_desc)} buku")

print("="*50)
print("Contoh 3 Data Teratas:")
print(df[['Title', 'google_rating', 'google_page_count']].head(3))