CREATE SCHEMA IF NOT EXISTS fct;

-- Payments aggregated at order level
DROP TABLE IF EXISTS fct._order_payments_agg;
CREATE TABLE fct._order_payments_agg AS
SELECT
  order_id,
  SUM(payment_value) AS order_payment_value,
  -- keep a dominant payment type (most frequent; tie-break alphabetical)
  (SELECT payment_type
   FROM (
     SELECT payment_type, COUNT(*) AS c
     FROM stg.order_payments p2
     WHERE p2.order_id = p.order_id
     GROUP BY payment_type
     ORDER BY c DESC, payment_type ASC
     LIMIT 1
   )
  ) AS dominant_payment_type
FROM stg.order_payments p
GROUP BY order_id;

-- Build order-item grain fact table with correct allocation
DROP TABLE IF EXISTS fct.fct_order_items;
CREATE TABLE fct.fct_order_items AS
WITH base AS (
  SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.order_status,
    o.order_purchase_ts,
    CAST(strftime(CAST(o.order_purchase_ts AS DATE), '%Y%m%d') AS BIGINT) AS order_purchase_date_id,
    oi.item_price,
    oi.freight_value,
    (oi.item_price + oi.freight_value) AS item_gmv,
    pa.order_payment_value,
    pa.dominant_payment_type
  FROM stg.order_items oi
  JOIN stg.orders o ON oi.order_id = o.order_id
  LEFT JOIN fct._order_payments_agg pa ON oi.order_id = pa.order_id
  WHERE o.order_status IN ('delivered','shipped','invoiced','processing','approved')
),
alloc AS (
  SELECT
    *,
    SUM(item_gmv) OVER (PARTITION BY order_id) AS order_gmv
  FROM base
),
enriched AS (
  SELECT
    a.*,
    CASE
      WHEN a.order_payment_value IS NULL THEN NULL
      WHEN a.order_gmv = 0 THEN NULL
      ELSE a.order_payment_value * (a.item_gmv / a.order_gmv)
    END AS allocated_payment_value
  FROM alloc a
),
with_product AS (
  SELECT
    e.*,
    dp.product_category_name_en,
    cr.cogs_rate,
    cr.payment_fee_rate
  FROM enriched e
  LEFT JOIN dim.dim_products dp ON e.product_id = dp.product_id
  LEFT JOIN dim.dim_cogs_rates cr ON dp.product_category_name_en = cr.product_category_name_en
)
SELECT
  -- keys
  order_id,
  order_item_id,
  customer_id,
  product_id,
  seller_id,
  order_purchase_date_id,

  -- context
  order_status,
  order_purchase_ts,
  dominant_payment_type,

  -- base measures
  item_price,
  freight_value,
  item_gmv,
  order_payment_value,
  allocated_payment_value,

  -- category
  product_category_name_en,

  -- assumptions
  cogs_rate,
  payment_fee_rate,

  -- derived economics (client-ready)
  (item_price * COALESCE(cogs_rate, 0.50)) AS estimated_cogs,
  (COALESCE(allocated_payment_value, item_gmv) * COALESCE(payment_fee_rate, 0.029)) AS estimated_payment_fees,

  -- contribution margin (definition: allocated revenue - cogs - payment fees - freight pass-through treated as revenue in allocation)
  (COALESCE(allocated_payment_value, item_gmv)
    - (item_price * COALESCE(cogs_rate, 0.50))
    - (COALESCE(allocated_payment_value, item_gmv) * COALESCE(payment_fee_rate, 0.029))
  ) AS contribution_margin
FROM with_product;

CREATE INDEX IF NOT EXISTS idx_fct_order_items_date ON fct.fct_order_items(order_purchase_date_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_product ON fct.fct_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_customer ON fct.fct_order_items(customer_id);
