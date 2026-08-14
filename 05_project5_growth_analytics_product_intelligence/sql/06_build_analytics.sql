-- =============================================================================
-- 06_build_analytics.sql
-- Growth Analytics & Product Intelligence — Analytics Layer
--
-- Builds on top of dim.* + fct.* to produce:
--   analytics.customer_ltv          — LTV milestones (30/60/90/180/365d)
--   analytics.cohort_retention      — Monthly cohort retention matrix
--   analytics.rfm_scores            — RFM quintile scores + segment labels
--   analytics.repeat_purchase_behavior — Inter-order intervals & velocity
--   analytics.churn_signals         — Full ML feature table + is_churned label
--   analytics.cac_simulation        — CAC by channel/month from simulated spend
--   analytics.churn_predictions     — Shell table (populated by Python ML step)
--   mart.executive_kpis             — MoM KPI rollup view
--   mart.product_performance        — Revenue, margin, quality by product
--   mart.customer_health            — Segment + health tier per customer
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS mart;

-- ─────────────────────────────────────────────────────────────────────────────
-- HELPER: _customer_order_base
-- Order-level summary joined to customer_unique_id.
-- Filtered to active order statuses only.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics._customer_order_base;
CREATE TABLE analytics._customer_order_base AS
WITH orders_per_customer AS (
    SELECT
        c.customer_unique_id,
        fi.order_id,
        CAST(fi.order_purchase_ts AS DATE)                          AS order_date,
        SUM(COALESCE(fi.allocated_payment_value, fi.item_gmv))      AS order_revenue,
        SUM(fi.contribution_margin)                                  AS order_margin
    FROM fct.fct_order_items fi
    JOIN dim.dim_customers c ON fi.customer_id = c.customer_id
    GROUP BY 1, 2, 3
)
SELECT
    customer_unique_id,
    order_id,
    order_date,
    order_revenue,
    order_margin,
    MIN(order_date)    OVER (PARTITION BY customer_unique_id) AS first_order_date,
    MAX(order_date)    OVER (PARTITION BY customer_unique_id) AS last_order_date,
    COUNT(order_id)    OVER (PARTITION BY customer_unique_id) AS lifetime_orders,
    SUM(order_revenue) OVER (PARTITION BY customer_unique_id) AS lifetime_revenue
FROM orders_per_customer;

-- Dataset end-date anchor — derived from data so no hardcoding is needed
DROP TABLE IF EXISTS analytics._dataset_end_date;
CREATE TABLE analytics._dataset_end_date AS
SELECT MAX(order_date) AS end_date FROM analytics._customer_order_base;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. analytics.customer_ltv
--    Cumulative revenue at LTV milestones post first order.
--    Single-order customers fall back to lifetime_revenue for predicted_ltv_12m.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.customer_ltv;
CREATE TABLE analytics.customer_ltv AS
WITH milestones AS (
    SELECT
        customer_unique_id,
        first_order_date,
        last_order_date,
        lifetime_orders,
        lifetime_revenue,
        SUM(CASE WHEN order_date <= first_order_date + INTERVAL 30 DAYS
                 THEN order_revenue ELSE 0 END) AS ltv_30d,
        SUM(CASE WHEN order_date <= first_order_date + INTERVAL 60 DAYS
                 THEN order_revenue ELSE 0 END) AS ltv_60d,
        SUM(CASE WHEN order_date <= first_order_date + INTERVAL 90 DAYS
                 THEN order_revenue ELSE 0 END) AS ltv_90d,
        SUM(CASE WHEN order_date <= first_order_date + INTERVAL 180 DAYS
                 THEN order_revenue ELSE 0 END) AS ltv_180d,
        SUM(CASE WHEN order_date <= first_order_date + INTERVAL 365 DAYS
                 THEN order_revenue ELSE 0 END) AS ltv_365d
    FROM analytics._customer_order_base
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    customer_unique_id,
    first_order_date,
    last_order_date,
    lifetime_orders,
    lifetime_revenue,
    ltv_30d,
    ltv_60d,
    ltv_90d,
    ltv_180d,
    ltv_365d,
    -- Predicted 12-month LTV: avg monthly revenue × 12
    -- Single-order customers: use their one observed order value (conservative lower bound)
    CASE
        WHEN lifetime_orders >= 2
        THEN ROUND(
            (lifetime_revenue / GREATEST(1, DATEDIFF('month', first_order_date, last_order_date)))
            * 12, 2)
        ELSE ROUND(lifetime_revenue, 2)
    END AS predicted_ltv_12m
FROM milestones;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. analytics.cohort_retention
--    Monthly cohort × periods_since_first matrix.
--    NOTE: Olist has ~3% repeat customer rate — retention curves will drop
--    steeply after period 0. This is analytically correct; document in README.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.cohort_retention;
CREATE TABLE analytics.cohort_retention AS
WITH cohorts AS (
    SELECT DISTINCT
        customer_unique_id,
        strftime(first_order_date, '%Y-%m') AS cohort_month
    FROM analytics._customer_order_base
),
customer_activity AS (
    SELECT DISTINCT
        customer_unique_id,
        strftime(order_date, '%Y-%m') AS activity_month
    FROM analytics._customer_order_base
),
joined AS (
    SELECT
        c.cohort_month,
        ca.activity_month,
        COUNT(DISTINCT c.customer_unique_id) AS active_users
    FROM cohorts c
    JOIN customer_activity ca ON c.customer_unique_id = ca.customer_unique_id
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT customer_unique_id) AS cohort_size
    FROM cohorts
    GROUP BY 1
),
periods AS (
    SELECT
        j.cohort_month,
        j.activity_month,
        j.active_users,
        cs.cohort_size,
        (
            EXTRACT(YEAR  FROM CAST(j.activity_month || '-01' AS DATE)) * 12 +
            EXTRACT(MONTH FROM CAST(j.activity_month || '-01' AS DATE))
        ) - (
            EXTRACT(YEAR  FROM CAST(j.cohort_month || '-01' AS DATE)) * 12 +
            EXTRACT(MONTH FROM CAST(j.cohort_month || '-01' AS DATE))
        ) AS periods_since_first
    FROM joined j
    JOIN cohort_sizes cs ON j.cohort_month = cs.cohort_month
)
SELECT
    cohort_month,
    activity_month,
    periods_since_first,
    cohort_size,
    active_users,
    ROUND(active_users::DOUBLE / cohort_size, 4) AS retention_rate,
    ROUND(1.0 - (active_users::DOUBLE / cohort_size), 4) AS churn_rate
FROM periods
WHERE periods_since_first >= 0
ORDER BY cohort_month, periods_since_first;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. analytics.rfm_scores
--    Recency / Frequency / Monetary quintile scoring (1–5) + 8 segment labels.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.rfm_scores;
CREATE TABLE analytics.rfm_scores AS
WITH rfm_raw AS (
    SELECT
        customer_unique_id,
        DATEDIFF('day', MAX(order_date),
                 (SELECT end_date FROM analytics._dataset_end_date)) AS recency_days,
        COUNT(DISTINCT order_id)                                       AS frequency,
        SUM(order_revenue)                                             AS monetary_total,
        AVG(order_revenue)                                             AS monetary_avg
    FROM analytics._customer_order_base
    GROUP BY 1
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,  -- lower recency → higher score
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary_total ASC) AS m_score
    FROM rfm_raw
)
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary_total,
    monetary_avg,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score) AS rfm_total_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4                           THEN 'champions'
        WHEN r_score >= 3 AND f_score >= 3                           THEN 'loyal'
        WHEN r_score >= 4 AND f_score <= 2                           THEN 'recent_new'
        WHEN r_score >= 3 AND f_score <= 2 AND m_score >= 3          THEN 'potential_loyalist'
        WHEN r_score <= 2 AND f_score >= 3                           THEN 'at_risk'
        WHEN r_score = 1  AND f_score >= 3                           THEN 'cant_lose'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3          THEN 'hibernating_high_value'
        WHEN r_score = 1  AND f_score = 1                            THEN 'lost'
        ELSE 'others'
    END AS rfm_segment
FROM rfm_scored;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. analytics.repeat_purchase_behavior
--    Per-customer inter-order intervals and purchase velocity.
--    Single-order customers get -1 sentinel for interval fields.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.repeat_purchase_behavior;
CREATE TABLE analytics.repeat_purchase_behavior AS
WITH ordered AS (
    SELECT
        customer_unique_id,
        order_id,
        order_date,
        order_revenue,
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_date) AS order_seq,
        LAG(order_date) OVER (PARTITION BY customer_unique_id ORDER BY order_date) AS prev_order_date
    FROM analytics._customer_order_base
),
intervals AS (
    SELECT
        customer_unique_id,
        DATEDIFF('day', prev_order_date, order_date) AS days_between_orders
    FROM ordered
    WHERE prev_order_date IS NOT NULL
),
base AS (
    SELECT DISTINCT
        customer_unique_id,
        lifetime_orders,
        lifetime_revenue,
        first_order_date,
        last_order_date
    FROM analytics._customer_order_base
)
SELECT
    b.customer_unique_id,
    b.lifetime_orders,
    b.lifetime_revenue,
    b.first_order_date,
    b.last_order_date,
    DATEDIFF('day', b.first_order_date, b.last_order_date) AS customer_lifespan_days,
    COALESCE(AVG(i.days_between_orders), -1)               AS inter_purchase_interval_avg,
    COALESCE(MIN(i.days_between_orders), -1)               AS min_inter_purchase_days,
    COALESCE(MAX(i.days_between_orders), -1)               AS max_inter_purchase_days,
    CASE
        WHEN DATEDIFF('day', b.first_order_date, b.last_order_date) > 0
        THEN ROUND(
            b.lifetime_orders::DOUBLE
            / GREATEST(1, DATEDIFF('day', b.first_order_date, b.last_order_date)) * 30, 4)
        ELSE 1.0
    END AS purchase_velocity_per_30d
FROM base b
LEFT JOIN intervals i ON b.customer_unique_id = i.customer_unique_id
GROUP BY 1, 2, 3, 4, 5, 6;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. analytics.churn_signals
--    Full ML feature table combining RFM + behavioral + product + satisfaction
--    + payment + temporal features.
--
--    Churn label: is_churned = 1 if recency_days > 90 (no purchase in last 90
--    days of the dataset window ending ~2018-10-17).
--
--    Class imbalance note: Olist's ~97% single-order rate means most customers
--    will be labelled churned. Use class_weight='balanced' in ML models and
--    prioritise ROC-AUC over accuracy.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.churn_signals;
CREATE TABLE analytics.churn_signals AS
WITH
-- Payment features: dominant type + avg installments per customer
payment_agg AS (
    SELECT
        c.customer_unique_id,
        -- mode of payment_type across all orders for this customer
        (SELECT sp2.payment_type
         FROM stg.order_payments sp2
         JOIN stg.orders so2 ON sp2.order_id = so2.order_id
         JOIN stg.customers sc2 ON so2.customer_id = sc2.customer_id
         WHERE sc2.customer_unique_id = c.customer_unique_id
         GROUP BY sp2.payment_type
         ORDER BY COUNT(*) DESC, sp2.payment_type ASC
         LIMIT 1
        ) AS preferred_payment_type,
        AVG(sp.payment_installments::DOUBLE) AS installment_avg
    FROM stg.customers c
    JOIN stg.orders so ON c.customer_id = so.customer_id
    JOIN stg.order_payments sp ON so.order_id = sp.order_id
    GROUP BY c.customer_unique_id
),
-- Review features: avg score + fraction of bad reviews
review_agg AS (
    SELECT
        c.customer_unique_id,
        AVG(r.review_score::DOUBLE)                                       AS avg_review_score,
        SUM(CASE WHEN r.review_score <= 2 THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(r.review_id), 0)                               AS pct_low_reviews
    FROM stg.customers c
    JOIN stg.orders so ON c.customer_id = so.customer_id
    JOIN raw.order_reviews r ON so.order_id = r.order_id
    GROUP BY c.customer_unique_id
),
-- Product diversity: unique products and categories purchased
product_div AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT fi.product_id)               AS unique_products,
        COUNT(DISTINCT fi.product_category_name_en) AS unique_categories
    FROM stg.customers c
    JOIN fct.fct_order_items fi ON c.customer_id = fi.customer_id
    GROUP BY c.customer_unique_id
),
-- Temporal purchase pattern: weekday vs weekend share
temporal_agg AS (
    SELECT
        c.customer_unique_id,
        COUNT(CASE WHEN EXTRACT(DOW FROM so.order_purchase_ts) BETWEEN 1 AND 5
                   THEN 1 END)::DOUBLE
            / NULLIF(COUNT(*), 0) AS weekday_purchase_pct,
        COUNT(CASE WHEN EXTRACT(DOW FROM so.order_purchase_ts) IN (0, 6)
                   THEN 1 END)::DOUBLE
            / NULLIF(COUNT(*), 0) AS weekend_purchase_pct
    FROM stg.customers c
    JOIN stg.orders so ON c.customer_id = so.customer_id
    GROUP BY c.customer_unique_id
)
SELECT
    rfm.customer_unique_id,
    -- RFM features
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary_total,
    rfm.monetary_avg,
    rfm.r_score,
    rfm.f_score,
    rfm.m_score,
    rfm.rfm_segment,
    -- Behavioral features
    rpb.customer_lifespan_days,
    rpb.inter_purchase_interval_avg,
    rpb.purchase_velocity_per_30d,
    -- Product diversity
    COALESCE(pd.unique_products,   0) AS unique_products,
    COALESCE(pd.unique_categories, 0) AS unique_categories,
    -- Satisfaction
    COALESCE(rev.avg_review_score, 3.0) AS avg_review_score,
    COALESCE(rev.pct_low_reviews,  0.0) AS pct_low_reviews,
    -- Payment behaviour
    COALESCE(pa.preferred_payment_type, 'credit_card') AS preferred_payment_type,
    COALESCE(pa.installment_avg, 1.0)                  AS installment_avg,
    -- Temporal patterns
    COALESCE(ta.weekday_purchase_pct, 0.8) AS weekday_purchase_pct,
    COALESCE(ta.weekend_purchase_pct, 0.2) AS weekend_purchase_pct,
    -- Churn label (90-day lookback from dataset end)
    CASE WHEN rfm.recency_days > 90 THEN 1 ELSE 0 END AS is_churned
FROM analytics.rfm_scores rfm
LEFT JOIN analytics.repeat_purchase_behavior rpb ON rfm.customer_unique_id = rpb.customer_unique_id
LEFT JOIN product_div pd   ON rfm.customer_unique_id = pd.customer_unique_id
LEFT JOIN review_agg  rev  ON rfm.customer_unique_id = rev.customer_unique_id
LEFT JOIN payment_agg pa   ON rfm.customer_unique_id = pa.customer_unique_id
LEFT JOIN temporal_agg ta  ON rfm.customer_unique_id = ta.customer_unique_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. analytics.cac_simulation
--    CAC by channel/month using simulated spend + new customer counts.
--    Blended CAC = total spend / new customers acquired that month.
--    Channel CAC proxy = spend-share × blended CAC (assumption documented).
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.cac_simulation;
CREATE TABLE analytics.cac_simulation AS
WITH monthly_spend AS (
    SELECT
        d.year_month,
        s.channel,
        SUM(s.spend) AS monthly_spend
    FROM fct.fct_marketing_spend s
    JOIN dim.dim_date d ON s.date_id = d.date_id
    GROUP BY 1, 2
),
monthly_new_customers AS (
    SELECT
        strftime(first_order_date, '%Y-%m') AS year_month,
        COUNT(DISTINCT customer_unique_id)  AS new_customers
    FROM analytics._customer_order_base
    WHERE order_date = first_order_date
    GROUP BY 1
),
total_monthly_spend AS (
    SELECT year_month, SUM(monthly_spend) AS total_spend
    FROM monthly_spend
    GROUP BY 1
)
SELECT
    ms.year_month,
    ms.channel,
    ms.monthly_spend,
    tms.total_spend,
    mnc.new_customers,
    CASE
        WHEN mnc.new_customers > 0 AND tms.total_spend > 0
        THEN ROUND(
            (ms.monthly_spend / tms.total_spend) * tms.total_spend / mnc.new_customers, 2)
        ELSE NULL
    END AS channel_cac_proxy,
    CASE
        WHEN mnc.new_customers > 0
        THEN ROUND(tms.total_spend / mnc.new_customers, 2)
        ELSE NULL
    END AS blended_cac
FROM monthly_spend ms
JOIN total_monthly_spend tms ON ms.year_month = tms.year_month
LEFT JOIN monthly_new_customers mnc ON ms.year_month = mnc.year_month
ORDER BY ms.year_month, ms.channel;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. analytics.churn_predictions
--    Shell table — populated by src/05_churn_model.py after model training.
--    Pre-created so downstream exports and Streamlit app never fail on missing table.
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS analytics.churn_predictions;
CREATE TABLE analytics.churn_predictions (
    customer_unique_id  VARCHAR,
    churn_probability   DOUBLE,
    predicted_churned   INTEGER,
    model_name          VARCHAR,
    scored_at           TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. mart.executive_kpis
--    Monthly KPI rollup: revenue MoM, median LTV, LTV:CAC ratio,
--    period-1 churn rate, period-1 retention rate.
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS mart.executive_kpis;
CREATE VIEW mart.executive_kpis AS
WITH monthly_revenue AS (
    SELECT
        d.year_month,
        SUM(COALESCE(fi.allocated_payment_value, fi.item_gmv)) AS revenue,
        COUNT(DISTINCT fi.order_id)                             AS orders,
        COUNT(DISTINCT fi.customer_id)                          AS customers
    FROM fct.fct_order_items fi
    JOIN dim.dim_date d ON fi.order_purchase_date_id = d.date_id
    GROUP BY 1
),
monthly_ltv AS (
    SELECT
        strftime(first_order_date, '%Y-%m')                                    AS year_month,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_revenue)          AS median_ltv,
        AVG(predicted_ltv_12m)                                                 AS avg_predicted_ltv_12m
    FROM analytics.customer_ltv
    GROUP BY 1
),
monthly_cac AS (
    SELECT year_month, AVG(blended_cac) AS avg_blended_cac
    FROM analytics.cac_simulation
    GROUP BY 1
),
monthly_churn AS (
    SELECT
        cohort_month AS year_month,
        AVG(CASE WHEN periods_since_first = 1 THEN churn_rate     END) AS period1_churn_rate,
        AVG(CASE WHEN periods_since_first = 1 THEN retention_rate END) AS period1_retention_rate
    FROM analytics.cohort_retention
    GROUP BY 1
)
SELECT
    mr.year_month,
    mr.revenue,
    mr.orders,
    mr.customers,
    LAG(mr.revenue) OVER (ORDER BY mr.year_month)                            AS prev_month_revenue,
    CASE
        WHEN LAG(mr.revenue) OVER (ORDER BY mr.year_month) > 0
        THEN ROUND(
            (mr.revenue - LAG(mr.revenue) OVER (ORDER BY mr.year_month))
            / LAG(mr.revenue) OVER (ORDER BY mr.year_month), 4)
        ELSE NULL
    END                                                                       AS revenue_mom_growth,
    ml.median_ltv,
    ml.avg_predicted_ltv_12m,
    mc.avg_blended_cac,
    CASE WHEN mc.avg_blended_cac > 0
         THEN ROUND(ml.median_ltv / mc.avg_blended_cac, 2) ELSE NULL
    END                                                                       AS ltv_cac_ratio,
    mch.period1_churn_rate,
    mch.period1_retention_rate
FROM monthly_revenue mr
LEFT JOIN monthly_ltv   ml  ON mr.year_month = ml.year_month
LEFT JOIN monthly_cac   mc  ON mr.year_month = mc.year_month
LEFT JOIN monthly_churn mch ON mr.year_month = mch.year_month
ORDER BY mr.year_month;

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. mart.product_performance
--    Revenue, contribution margin, and quality signals by product + category.
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS mart.product_performance;
CREATE VIEW mart.product_performance AS
WITH product_metrics AS (
    SELECT
        fi.product_id,
        dp.product_category_name_en                              AS category,
        COUNT(DISTINCT fi.order_id)                              AS total_orders,
        SUM(COALESCE(fi.allocated_payment_value, fi.item_gmv))   AS total_revenue,
        SUM(fi.estimated_cogs)                                   AS total_cogs,
        SUM(fi.contribution_margin)                              AS total_contribution_margin,
        AVG(fi.item_price)                                       AS avg_item_price
    FROM fct.fct_order_items fi
    LEFT JOIN dim.dim_products dp ON fi.product_id = dp.product_id
    GROUP BY 1, 2
),
low_review_rates AS (
    SELECT
        fi.product_id,
        COUNT(DISTINCT CASE WHEN r.review_score = 1 THEN fi.order_id END)::DOUBLE
            / NULLIF(COUNT(DISTINCT fi.order_id), 0) AS low_review_rate
    FROM fct.fct_order_items fi
    LEFT JOIN raw.order_reviews r ON fi.order_id = r.order_id
    GROUP BY 1
)
SELECT
    pm.product_id,
    pm.category,
    pm.total_orders,
    pm.total_revenue,
    pm.total_cogs,
    pm.total_contribution_margin,
    pm.avg_item_price,
    ROUND(pm.total_contribution_margin / NULLIF(pm.total_revenue, 0), 4) AS cm_margin_pct,
    COALESCE(lr.low_review_rate, 0)                                       AS low_review_rate
FROM product_metrics pm
LEFT JOIN low_review_rates lr ON pm.product_id = lr.product_id
ORDER BY pm.total_revenue DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 10. mart.customer_health
--     Full customer view: segment, health tier, LTV, churn signal.
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS mart.customer_health;
CREATE VIEW mart.customer_health AS
SELECT
    rfm.customer_unique_id,
    rfm.rfm_segment,
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary_total,
    rfm.rfm_total_score,
    ltv.lifetime_orders,
    ltv.lifetime_revenue,
    ltv.first_order_date,
    ltv.last_order_date,
    ltv.predicted_ltv_12m,
    cs.is_churned,
    cs.avg_review_score,
    cs.pct_low_reviews,
    cs.preferred_payment_type,
    cs.inter_purchase_interval_avg,
    CASE
        WHEN rfm.rfm_segment IN ('champions', 'loyal')               THEN 'healthy'
        WHEN rfm.rfm_segment IN ('recent_new', 'potential_loyalist') THEN 'growing'
        WHEN rfm.rfm_segment IN ('at_risk', 'hibernating_high_value') THEN 'at_risk'
        WHEN rfm.rfm_segment IN ('cant_lose')                        THEN 'critical'
        ELSE 'lost'
    END AS health_tier
FROM analytics.rfm_scores rfm
LEFT JOIN analytics.customer_ltv    ltv ON rfm.customer_unique_id = ltv.customer_unique_id
LEFT JOIN analytics.churn_signals   cs  ON rfm.customer_unique_id = cs.customer_unique_id;
