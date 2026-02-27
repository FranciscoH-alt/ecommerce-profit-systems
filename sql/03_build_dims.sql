CREATE SCHEMA IF NOT EXISTS dim;

-- dim_date
DROP TABLE IF EXISTS dim.dim_date;
CREATE TABLE dim.dim_date AS
WITH bounds AS (
  SELECT
    CAST(date_trunc('day', MIN(order_purchase_ts)) AS DATE) AS min_date,
    CAST(date_trunc('day', MAX(order_purchase_ts)) AS DATE) AS max_date
  FROM stg.orders
),
dates AS (
  SELECT * FROM generate_series(
    (SELECT min_date FROM bounds),
    (SELECT max_date FROM bounds),
    INTERVAL 1 DAY
  ) AS t(d)
)
SELECT
  CAST(strftime(d, '%Y%m%d') AS BIGINT) AS date_id,
  d AS date,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(MONTH FROM d) AS month,
  EXTRACT(DAY FROM d) AS day,
  EXTRACT(DOW FROM d) AS dow,
  EXTRACT(WEEK FROM d) AS week_of_year,
  EXTRACT(QUARTER FROM d) AS quarter,
  strftime(d, '%Y-%m') AS year_month
FROM dates;

-- dim_customers
DROP TABLE IF EXISTS dim.dim_customers;
CREATE TABLE dim.dim_customers AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM stg.customers;

-- dim_products (with english category)
DROP TABLE IF EXISTS dim.dim_products;
CREATE TABLE dim.dim_products AS
SELECT
  p.product_id,
  p.product_category_name_pt,
  COALESCE(t.product_category_name_en, 'unknown') AS product_category_name_en,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM stg.products p
LEFT JOIN stg.category_translation t
  ON p.product_category_name_pt = t.product_category_name_pt;

-- dim_channels (used by marketing spend)
DROP TABLE IF EXISTS dim.dim_channels;
CREATE TABLE dim.dim_channels AS
SELECT * FROM (VALUES
  ('Meta'),
  ('Google'),
  ('TikTok'),
  ('Email')
) AS v(channel);

-- COGS rate assumptions (category-level)
-- Client-ready approach: explicit assumption table; you can tune later.
DROP TABLE IF EXISTS dim.dim_cogs_rates;
CREATE TABLE dim.dim_cogs_rates AS
SELECT
  product_category_name_en,
  CASE
    WHEN product_category_name_en IN ('computers_accessories','electronics','telephony') THEN 0.62
    WHEN product_category_name_en IN ('watches_gifts','fashion_bags_accessories','fashion_shoes') THEN 0.48
    WHEN product_category_name_en IN ('health_beauty','perfumery') THEN 0.42
    WHEN product_category_name_en IN ('bed_bath_table','furniture_decor','housewares') THEN 0.55
    WHEN product_category_name_en IN ('sports_leisure') THEN 0.50
    WHEN product_category_name_en IN ('toys','baby') THEN 0.52
    ELSE 0.50
  END AS cogs_rate,
  0.029 AS payment_fee_rate  -- generic processing fee assumption
FROM (SELECT DISTINCT product_category_name_en FROM dim.dim_products);

-- Helpful indexes (DuckDB supports them; optional but fine)
CREATE INDEX IF NOT EXISTS idx_dim_date_date_id ON dim.dim_date(date_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_customer_id ON dim.dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_product_id ON dim.dim_products(product_id);
