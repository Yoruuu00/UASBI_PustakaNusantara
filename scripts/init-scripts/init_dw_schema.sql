-- ============================================================================
-- DATA WAREHOUSE SCHEMA - STAR SCHEMA
-- Save this as: init-scripts/init_dw_schema.sql
-- ============================================================================

-- Drop existing tables
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_book CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_category CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_profit_range CASCADE;
DROP TABLE IF EXISTS staging_oltp_data CASCADE;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- DIM_DATE
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    week_of_year INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- DIM_BOOK
CREATE TABLE dim_book (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    title_clean VARCHAR(500),
    api_found BOOLEAN DEFAULT FALSE,
    api_title VARCHAR(500),
    api_author VARCHAR(200),
    api_authors_all TEXT,
    api_first_publish_year INT,
    api_isbn VARCHAR(20),
    api_isbn_all TEXT,
    api_publisher VARCHAR(200),
    api_publishers_all TEXT,
    api_language VARCHAR(10),
    api_number_of_pages INT,
    api_subject TEXT,
    api_edition_count INT,
    api_has_fulltext BOOLEAN,
    api_search_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_book_title ON dim_book(title);
CREATE INDEX idx_dim_book_api_found ON dim_book(api_found);

-- DIM_CUSTOMER
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) NOT NULL UNIQUE,
    first_purchase_date DATE,
    total_purchases INT DEFAULT 0,
    total_amount_spent DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_code ON dim_customer(customer_code);

-- DIM_CATEGORY
CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    category_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_category_name ON dim_category(category_name);

-- DIM_LOCATION
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(100) DEFAULT 'India',
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, state)
);

CREATE INDEX idx_dim_location_city ON dim_location(city);
CREATE INDEX idx_dim_location_state ON dim_location(state);

-- DIM_PROFIT_RANGE
CREATE TABLE dim_profit_range (
    profit_range_id SERIAL PRIMARY KEY,
    range_name VARCHAR(50) NOT NULL UNIQUE,
    min_profit DECIMAL(10,2) NOT NULL,
    max_profit DECIMAL(10,2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_profit_range (range_name, min_profit, max_profit, description) VALUES
('Very Low', 0, 20, 'Profit between 0-20 INR'),
('Low', 20, 50, 'Profit between 20-50 INR'),
('Medium', 50, 100, 'Profit between 50-100 INR'),
('High', 100, 200, 'Profit between 100-200 INR'),
('Very High', 200, 999999, 'Profit above 200 INR');

-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    book_id INT NOT NULL REFERENCES dim_book(book_id),
    customer_id INT NOT NULL REFERENCES dim_customer(customer_id),
    category_id INT NOT NULL REFERENCES dim_category(category_id),
    location_id INT NOT NULL REFERENCES dim_location(location_id),
    profit_range_id INT REFERENCES dim_profit_range(profit_range_id),
    quantity INT NOT NULL,
    item_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    profit DECIMAL(10,2) NOT NULL,
    profit_margin DECIMAL(5,2),
    purchase_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (quantity > 0),
    CHECK (item_price >= 0),
    CHECK (total_amount >= 0),
    CHECK (profit >= 0)
);

CREATE INDEX idx_fact_sales_date_id ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_book_id ON fact_sales(book_id);
CREATE INDEX idx_fact_sales_customer_id ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_category_id ON fact_sales(category_id);
CREATE INDEX idx_fact_sales_location_id ON fact_sales(location_id);
CREATE INDEX idx_fact_sales_purchase_date ON fact_sales(purchase_date);

-- ============================================================================
-- STAGING TABLE
-- ============================================================================

CREATE TABLE staging_oltp_data (
    id SERIAL PRIMARY KEY,
    purchase_date VARCHAR(50),
    customer_id VARCHAR(50),
    title TEXT,
    quantity INT,
    item_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    profit DECIMAL(10,2),
    category VARCHAR(100),
    ship_city VARCHAR(100),
    ship_state VARCHAR(100),
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_staging_processed ON staging_oltp_data(processed);

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE OR REPLACE VIEW vw_sales_by_category AS
SELECT 
    c.category_name,
    COUNT(*) as total_sales,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue,
    SUM(f.profit) as total_profit,
    AVG(f.profit_margin) as avg_profit_margin
FROM fact_sales f
JOIN dim_category c ON f.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_sales_by_date AS
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    COUNT(*) as total_sales,
    SUM(f.total_amount) as total_revenue,
    SUM(f.profit) as total_profit
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date, d.year, d.month, d.month_name, d.quarter
ORDER BY d.full_date;

CREATE OR REPLACE VIEW vw_top_books AS
SELECT 
    b.title,
    b.api_author,
    COUNT(*) as total_sales,
    SUM(f.total_amount) as total_revenue,
    SUM(f.profit) as total_profit
FROM fact_sales f
JOIN dim_book b ON f.book_id = b.book_id
GROUP BY b.book_id, b.title, b.api_author
ORDER BY total_revenue DESC
LIMIT 10;

CREATE OR REPLACE VIEW vw_sales_by_location AS
SELECT 
    l.state,
    l.city,
    COUNT(*) as total_sales,
    SUM(f.total_amount) as total_revenue,
    SUM(f.profit) as total_profit
FROM fact_sales f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.state, l.city
ORDER BY total_revenue DESC;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION get_data_quality_report()
RETURNS TABLE (
    metric_name VARCHAR(100),
    metric_value TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'Total Sales Records'::VARCHAR(100), COUNT(*)::TEXT FROM fact_sales
    UNION ALL
    SELECT 'Total Books'::VARCHAR(100), COUNT(*)::TEXT FROM dim_book
    UNION ALL
    SELECT 'Books with API Data'::VARCHAR(100), COUNT(*)::TEXT FROM dim_book WHERE api_found = TRUE
    UNION ALL
    SELECT 'Total Customers'::VARCHAR(100), COUNT(*)::TEXT FROM dim_customer
    UNION ALL
    SELECT 'Total Categories'::VARCHAR(100), COUNT(*)::TEXT FROM dim_category
    UNION ALL
    SELECT 'Total Locations'::VARCHAR(100), COUNT(*)::TEXT FROM dim_location
    UNION ALL
    SELECT 'Date Range'::VARCHAR(100), 
           COALESCE(MIN(full_date)::TEXT || ' to ' || MAX(full_date)::TEXT, 'No data') 
    FROM dim_date
    UNION ALL
    SELECT 'Total Revenue'::VARCHAR(100), 
           'INR ' || COALESCE(ROUND(SUM(total_amount)::NUMERIC, 2)::TEXT, '0') FROM fact_sales
    UNION ALL
    SELECT 'Total Profit'::VARCHAR(100), 
           'INR ' || COALESCE(ROUND(SUM(profit)::NUMERIC, 2)::TEXT, '0') FROM fact_sales;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dwuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dwuser;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO dwuser;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE fact_sales IS 'Fact table containing all book sales transactions';
COMMENT ON TABLE dim_book IS 'Dimension table for book information with API enrichment';
COMMENT ON TABLE dim_customer IS 'Dimension table for customer information';
COMMENT ON TABLE dim_category IS 'Dimension table for book categories';
COMMENT ON TABLE dim_location IS 'Dimension table for shipping locations';
COMMENT ON TABLE dim_date IS 'Dimension table for date/time information';
COMMENT ON TABLE staging_oltp_data IS 'Staging table for ETL process';