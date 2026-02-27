CREATE SCHEMA IF NOT EXISTS fct;
CREATE SCHEMA IF NOT EXISTS mart;

-- Simulated marketing spend (daily, channel mix, seasonality)
-- Deterministic randomness for repeatable demos
SELECT setseed(0.42);

DROP TABLE IF EXISTS fct.fct_marketing_spend;
CREATE TABLE fct.fct_marketing_spend AS
WITH d AS (
  SELECT date, date_id, dow, year_month
  FROM dim.dim_date
),
base AS (
  SELECT
    d.date_id,
    d.date,
    c.channel,
    -- baseline spend by channel
    CASE c.channel
      WHEN 'Meta' THEN 1200
      WHEN 'Google' THEN 900
      WHEN 'TikTok' THEN 700
      WHEN 'Email' THEN 120
      ELSE 300
    END AS base_spend,
    -- weekday uplift (Mon-Fri higher)
    CASE WHEN d.dow IN (1,2,3,4,5) THEN 1.12 ELSE 0.88 END AS weekday_factor,
    -- monthly seasonality (simple sine wave by month)
    (1.0 + 0.10 * sin((EXTRACT(MONTH FROM d.date)::DOUBLE / 12.0) * 2.0 * pi())) AS seasonality_factor,
    -- noise
    (0.85 + random() * 0.35) AS noise_factor
  FROM d
  CROSS JOIN dim.dim_channels c
)
SELECT
  date_id,
  date,
  channel,
  ROUND(base_spend * weekday_factor * seasonality_factor * noise_factor, 2) AS spend
FROM base;

CREATE INDEX IF NOT EXISTS idx_spend_date ON fct.fct_marketing_spend(date_id);

-- Executive marts (Tableau-ready)

-- 1) Daily exec: revenue, contribution margin, spend, MER, blended CAC proxies
DROP VIEW IF EXISTS mart.mart_daily_exec;
CREATE VIEW mart.mart_daily_exec AS
WITH sales AS (
  SELECT
    order_purchase_date_id AS date_id,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(COALESCE(allocated_payment_value, item_gmv)) AS revenue,
    SUM(estimated_cogs) AS est_cogs,
    SUM(estimated_payment_fees) AS est_payment_fees,
    SUM(contribution_margin) AS contribution_margin
  FROM fct.fct_order_items
  GROUP BY 1
),
spend AS (
  SELECT
    date_id,
    SUM(spend) AS spend
  FROM fct.fct_marketing_spend
  GROUP BY 1
)
SELECT
  d.date,
  d.year,
  d.month,
  d.year_month,
  s.orders,
  s.customers,
  s.revenue,
  s.est_cogs,
  s.est_payment_fees,
  s.contribution_margin,
  sp.spend,
  CASE WHEN sp.spend > 0 THEN s.revenue / sp.spend ELSE NULL END AS mer,
  CASE WHEN sp.spend > 0 THEN s.contribution_margin / sp.spend ELSE NULL END AS cm_roas,
  CASE WHEN s.orders > 0 THEN sp.spend / s.orders ELSE NULL END AS blended_cac_per_order
FROM dim.dim_date d
LEFT JOIN sales s ON d.date_id = s.date_id
LEFT JOIN spend sp ON d.date_id = sp.date_id
WHERE d.date BETWEEN (SELECT MIN(date) FROM dim.dim_date) AND (SELECT MAX(date) FROM dim.dim_date);

-- 2) Product exec: category rollup
DROP VIEW IF EXISTS mart.mart_product_exec;
CREATE VIEW mart.mart_product_exec AS
SELECT
  order_purchase_date_id AS date_id,
  dp.product_category_name_en AS category,
  COUNT(DISTINCT order_id) AS orders,
  SUM(COALESCE(allocated_payment_value, item_gmv)) AS revenue,
  SUM(estimated_cogs) AS est_cogs,
  SUM(contribution_margin) AS contribution_margin,
  AVG(COALESCE(cogs_rate, 0.50)) AS avg_cogs_rate
FROM fct.fct_order_items fi
LEFT JOIN dim.dim_products dp ON fi.product_id = dp.product_id
GROUP BY 1, 2;

-- 3) Channel exec: allocate spend by day/channel + show MER by channel using naive allocation
-- NOTE: Without true attribution, revenue-by-channel is a modeled assumption. Client-ready: label it as proxy.
DROP VIEW IF EXISTS mart.mart_channel_exec;
CREATE VIEW mart.mart_channel_exec AS
WITH spend AS (
  SELECT date_id, channel, SUM(spend) AS spend
  FROM fct.fct_marketing_spend
  GROUP BY 1,2
),
daily_sales AS (
  SELECT
    order_purchase_date_id AS date_id,
    SUM(COALESCE(allocated_payment_value, item_gmv)) AS revenue,
    SUM(contribution_margin) AS contribution_margin
  FROM fct.fct_order_items
  GROUP BY 1
),
spend_totals AS (
  SELECT date_id, SUM(spend) AS spend_total
  FROM spend
  GROUP BY 1
)
SELECT
  s.date_id,
  d.date,
  s.channel,
  s.spend,
  ds.revenue AS total_revenue,
  ds.contribution_margin AS total_contribution_margin,
  -- proxy allocation: distribute revenue in proportion to spend share (explicitly a proxy)
  CASE WHEN st.spend_total > 0 THEN ds.revenue * (s.spend / st.spend_total) ELSE NULL END AS revenue_proxy,
  CASE WHEN st.spend_total > 0 THEN ds.contribution_margin * (s.spend / st.spend_total) ELSE NULL END AS contribution_margin_proxy,
  CASE WHEN s.spend > 0 AND st.spend_total > 0 THEN (ds.revenue * (s.spend / st.spend_total)) / s.spend ELSE NULL END AS mer_proxy
FROM spend s
JOIN dim.dim_date d ON s.date_id = d.date_id
LEFT JOIN daily_sales ds ON s.date_id = ds.date_id
LEFT JOIN spend_totals st ON s.date_id = st.date_id;

-- 4) Customer exec: repeat behavior (simple)
DROP VIEW IF EXISTS mart.mart_customer_exec;
CREATE VIEW mart.mart_customer_exec AS
WITH customer_orders AS (
  SELECT
    customer_id,
    MIN(CAST(order_purchase_ts AS DATE)) AS first_order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(COALESCE(allocated_payment_value, item_gmv)) AS revenue,
    SUM(contribution_margin) AS contribution_margin
  FROM fct.fct_order_items
  GROUP BY 1
)
SELECT
  customer_id,
  first_order_date,
  total_orders,
  revenue,
  contribution_margin,
  CASE WHEN total_orders >= 2 THEN 1 ELSE 0 END AS is_repeat_customer
FROM customer_orders;
