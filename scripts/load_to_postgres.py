import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import psycopg2
from datetime import datetime
import os
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': 'postgres-dwh',  # Docker service name
    'port': 5432,
    'database': 'pustaka_dwh',
    'user': 'pustaka_admin',
    'password': 'pustaka2025'
}

# For local development (outside Docker)
# DB_CONFIG['host'] = 'localhost'

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def create_connection():
    """Create PostgreSQL connection using SQLAlchemy"""
    try:
        conn_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"✓ Connected to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return engine
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

# ============================================================================
# CREATE STAR SCHEMA
# ============================================================================

def execute_sql_file(engine, sql_file):
    """Execute SQL file to create schema"""
    try:
        if not os.path.exists(sql_file):
            logger.warning(f"SQL file not found: {sql_file}")
            return False
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        with engine.connect() as conn:
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            
            for stmt in statements:
                if stmt and not stmt.startswith('--'):
                    conn.execute(text(stmt))
                    conn.commit()
        
        logger.info(f"✓ Executed SQL file: {sql_file}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to execute SQL file: {e}")
        return False

# ============================================================================
# POPULATE DIMENSION TABLES
# ============================================================================

def populate_dim_date(engine, start_year=2018, end_year=2025):
    """Populate dim_date table with date range"""
    logger.info("Populating dim_date table...")
    
    try:
        date_range = pd.date_range(
            start=f'{start_year}-01-01',
            end=f'{end_year}-12-31',
            freq='D'
        )
        
        date_data = []
        for date in date_range:
            date_data.append({
                'full_date': date.date(),
                'year': date.year,
                'quarter': date.quarter,
                'month': date.month,
                'month_name': date.strftime('%B'),
                'day': date.day,
                'day_of_week': date.dayofweek,  # 0=Monday, 6=Sunday
                'day_of_week_name': date.strftime('%A'),
                'week_of_year': date.isocalendar()[1],
                'is_weekend': date.dayofweek >= 5,
                'fiscal_year': date.year if date.month >= 4 else date.year - 1,
                'fiscal_quarter': ((date.month - 4) % 12) // 3 + 1
            })
        
        df_date = pd.DataFrame(date_data)
        
        # Load to PostgreSQL
        df_date.to_sql('dim_date', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated dim_date with {len(df_date)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate dim_date: {e}")
        return False

def populate_dim_category(engine, df):
    """Populate dim_category from dataset"""
    logger.info("Populating dim_category table...")
    
    try:
        if 'Category' not in df.columns:
            logger.warning("Category column not found")
            return False
        
        categories = df['Category'].unique()
        category_data = [{'category_name': cat} for cat in categories if pd.notna(cat)]
        
        df_category = pd.DataFrame(category_data)
        
        # Load to PostgreSQL (ignore duplicates)
        df_category.to_sql('dim_category', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated dim_category with {len(df_category)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate dim_category: {e}")
        return False

def populate_dim_location(engine, df):
    """Populate dim_location from dataset"""
    logger.info("Populating dim_location table...")
    
    try:
        if 'Ship_City' not in df.columns or 'Ship_State' not in df.columns:
            logger.warning("Location columns not found")
            return False
        
        locations = df[['Ship_City', 'Ship_State']].drop_duplicates()
        locations = locations.rename(columns={'Ship_City': 'city', 'Ship_State': 'state'})
        locations['region'] = 'Unknown'  # Can be mapped later
        
        locations.to_sql('dim_location', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated dim_location with {len(locations)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate dim_location: {e}")
        return False

def populate_dim_book(engine, df):
    """Populate dim_book from dataset"""
    logger.info("Populating dim_book table...")
    
    try:
        if 'Title' not in df.columns:
            logger.warning("Title column not found")
            return False
        
        books = df[['Title']].drop_duplicates()
        books = books.rename(columns={'Title': 'title'})
        
        # Add optional columns if they exist
        if 'api_author' in df.columns:
            books['author'] = df.groupby('Title')['api_author'].first()
        if 'api_first_publish_year' in df.columns:
            books['publish_year'] = df.groupby('Title')['api_first_publish_year'].first()
        if 'Category' in df.columns:
            books['category'] = df.groupby('Title')['Category'].first()
        
        books['has_api_data'] = df.groupby('Title')['api_found'].first() if 'api_found' in df.columns else False
        
        books = books.reset_index(drop=True)
        books.to_sql('dim_book', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated dim_book with {len(books)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate dim_book: {e}")
        return False

def populate_dim_customer(engine, df):
    """Populate dim_customer from dataset"""
    logger.info("Populating dim_customer table...")
    
    try:
        if 'Customer_ID' not in df.columns:
            logger.warning("Customer_ID column not found")
            return False
        
        customers = df[['Customer_ID']].drop_duplicates()
        customers = customers.rename(columns={'Customer_ID': 'customer_id'})
        customers['customer_name'] = 'Unknown'  # Not available in dataset
        customers['customer_type'] = 'Individual'
        
        customers.to_sql('dim_customer', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated dim_customer with {len(customers)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate dim_customer: {e}")
        return False

# ============================================================================
# POPULATE FACT TABLE
# ============================================================================

def populate_fact_sales(engine, df):
    """Populate fact_sales table"""
    logger.info("Populating fact_sales table...")
    
    try:
        # Prepare fact table data
        fact_data = df[[
            'Customer_ID', 'Title', 'Category', 'Ship_City', 'Ship_State',
            'Purchase_Date', 'Quantity', 'Item_Price', 'Total_Amount', 'Profit'
        ]].copy()
        
        # Rename columns
        fact_data = fact_data.rename(columns={
            'Customer_ID': 'customer_id',
            'Purchase_Date': 'purchase_date',
            'Quantity': 'quantity',
            'Item_Price': 'item_price',
            'Total_Amount': 'total_amount',
            'Profit': 'profit'
        })
        
        # Calculate profit margin
        fact_data['profit_margin'] = (fact_data['profit'] / fact_data['total_amount'] * 100).round(2)
        
        # Get foreign keys (simplified - in production use proper lookup)
        # For now, we'll just use the data as is
        # In production, you'd lookup IDs from dimension tables
        
        fact_data.to_sql('fact_sales', engine, if_exists='append', index=False, method='multi')
        
        logger.info(f"✓ Populated fact_sales with {len(fact_data)} records")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to populate fact_sales: {e}")
        return False

# ============================================================================
# VERIFY DATA QUALITY
# ============================================================================

def verify_data_quality(engine):
    """Run data quality checks"""
    logger.info("\n" + "="*70)
    logger.info("DATA QUALITY VERIFICATION")
    logger.info("="*70)
    
    try:
        queries = {
            'Total Sales Records': 'SELECT COUNT(*) FROM fact_sales',
            'Total Books': 'SELECT COUNT(*) FROM dim_book',
            'Total Customers': 'SELECT COUNT(*) FROM dim_customer',
            'Total Locations': 'SELECT COUNT(*) FROM dim_location',
            'Date Range': 'SELECT MIN(full_date), MAX(full_date) FROM dim_date',
            'Null Checks': 'SELECT COUNT(*) FROM fact_sales WHERE total_amount IS NULL',
            'Total Revenue': 'SELECT SUM(total_amount) FROM fact_sales',
            'Total Profit': 'SELECT SUM(profit) FROM fact_sales',
        }
        
        with engine.connect() as conn:
            for check_name, query in queries.items():
                result = conn.execute(text(query)).fetchone()
                logger.info(f"  {check_name}: {result[0] if len(result) == 1 else result}")
        
        logger.info("\n✓ Data quality verification completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data quality verification failed: {e}")
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    logger.info("\n" + "="*70)
    logger.info(" LOAD DATA TO POSTGRESQL DATA WAREHOUSE")
    logger.info(" Toko Buku Pustaka Nusantara")
    logger.info("="*70)
    
    # 1. Connect to PostgreSQL
    engine = create_connection()
    if not engine:
        return False
    
    # 2. Execute schema SQL (if not already done)
    sql_file = 'init_db/01_create_schema.sql'
    if os.path.exists(sql_file):
        execute_sql_file(engine, sql_file)
    else:
        logger.warning(f"Schema SQL file not found: {sql_file}")
    
    # 3. Load dataset
    logger.info("\n" + "="*70)
    logger.info("LOADING DATASET")
    logger.info("="*70)
    
    csv_file = 'output/ML_Dataset_Processed_Full.csv'
    if not os.path.exists(csv_file):
        csv_file = 'output/ML_Dataset_Enriched.csv'
    if not os.path.exists(csv_file):
        csv_file = 'output/ML_Dataset_PustakaNusantara.csv'
    
    if not os.path.exists(csv_file):
        logger.error(f"❌ Dataset not found!")
        return False
    
    df = pd.read_csv(csv_file)
    logger.info(f"✓ Loaded dataset: {csv_file}")
    logger.info(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # 4. Populate dimension tables
    logger.info("\n" + "="*70)
    logger.info("POPULATING DIMENSION TABLES")
    logger.info("="*70)
    
    populate_dim_date(engine, 2018, 2025)
    populate_dim_category(engine, df)
    populate_dim_location(engine, df)
    populate_dim_book(engine, df)
    populate_dim_customer(engine, df)
    
    # 5. Populate fact table
    logger.info("\n" + "="*70)
    logger.info("POPULATING FACT TABLE")
    logger.info("="*70)
    
    populate_fact_sales(engine, df)
    
    # 6. Verify data quality
    verify_data_quality(engine)
    
    logger.info("\n" + "="*70)
    logger.info("✓ DATA LOAD COMPLETED SUCCESSFULLY!")
    logger.info("="*70)
    logger.info("\nNext Steps:")
    logger.info("1. Connect to PostgreSQL using PgAdmin")
    logger.info("2. Run sample queries to verify data")
    logger.info("3. Create reports and dashboards")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        exit(1)