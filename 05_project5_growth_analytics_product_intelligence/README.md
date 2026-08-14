# Ecommerce Growth Analytics & Product Intelligence Platform

> **Stack:** Python · SQL · DuckDB · scikit-learn · Streamlit  
> **Dataset:** [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100K+ orders, 2016–2018

---

## Overview

A production-style analytics platform built on a file-based DuckDB warehouse. Ingests raw Olist order, customer, product, and payment data, constructs a multi-layer analytical data model, trains a churn prediction model, and runs a statistically rigorous A/B pricing experiment — all orchestrated through a single command.

**Key capabilities:**

| Capability | Description |
|---|---|
| Warehouse engineering | 6-layer DuckDB pipeline: raw → stg → dim → fct → analytics → mart |
| Customer LTV | Cumulative LTV milestones at 30 / 60 / 90 / 180 / 365 days + predicted 12-month LTV |
| Cohort retention | Monthly cohort × period retention matrix with churn rates |
| RFM segmentation | Quintile-scored R/F/M with 8 actionable segment labels |
| CAC simulation | Blended and channel-attributed CAC from simulated marketing spend |
| Churn prediction | GradientBoostingClassifier + RandomForest with feature importance |
| A/B testing | Pricing experiment framework: chi-squared, Welch's t-test, Cohen's d, 95% CIs |
| Streamlit app | 4-page interactive dashboard for KPIs, cohort heatmap, churn explorer, and A/B results |

---

## Architecture

```
data/raw/*.csv  (9 files, ~100K orders)
      │
      ▼  01_load_raw.py
   raw.*  (9 tables, untyped)
      │
      ▼  02_clean.py
   stg.*  (typed, nulls removed, timestamps cast)
      │
      ▼  sql/03_build_dims.sql
   dim.*  (dim_date, dim_customers, dim_products, dim_channels, dim_cogs_rates)
      │
      ▼  sql/04_build_fact_orders.sql
   fct.*  (fct_order_items: allocated payments, contribution_margin)
      │
      ▼  sql/05_marketing_spend_and_marts.sql
   fct.fct_marketing_spend  +  mart.mart_daily/product/channel/customer_exec
      │
      ▼  sql/06_build_analytics.sql
   analytics.customer_ltv            (LTV milestones + predicted_ltv_12m)
   analytics.cohort_retention        (monthly retention matrix)
   analytics.rfm_scores              (R/F/M quintile scoring + 8 segments)
   analytics.repeat_purchase_behavior (inter-order intervals, velocity)
   analytics.churn_signals           (ML feature table + is_churned label)
   analytics.cac_simulation          (blended CAC by channel/month)
   analytics.churn_predictions       (filled by Python ML step)
   mart.executive_kpis               (MoM revenue, LTV:CAC, churn/retention)
   mart.product_performance          (revenue, margin, quality by product)
   mart.customer_health              (segment + health tier)
      │
      ├──▶  04_feature_engineering.py  →  features.parquet
      │
      ├──▶  05_churn_model.py          →  churn_model.pkl  +  DuckDB predictions
      │
      ├──▶  06_ab_testing.py           →  ab_test_report.csv  +  ab_test_stats.csv
      │
      └──▶  07_export.py               →  data/processed/*.csv + *.parquet
                                             │
                                             ▼
                                    app/streamlit_app.py
```

---

## Dataset Setup

1. Create a [Kaggle account](https://www.kaggle.com) and generate an API token (`~/.kaggle/kaggle.json`).

2. Download and unzip the dataset:
   ```bash
   pip install kaggle
   kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p data/raw/
   ```

3. Verify you have all 9 CSV files in `data/raw/`:
   ```
   olist_customers_dataset.csv
   olist_geolocation_dataset.csv
   olist_order_items_dataset.csv
   olist_order_payments_dataset.csv
   olist_order_reviews_dataset.csv
   olist_orders_dataset.csv
   olist_products_dataset.csv
   olist_sellers_dataset.csv
   product_category_name_translation.csv
   ```

---

## Quick Start

```bash
# 1. Install dependencies
pip3 install -r requirements.txt

# 2. Run the full pipeline (~3–5 min)
python3 run_all.py

# 3. Launch the Streamlit app
python3 -m streamlit run app/streamlit_app.py
```

To run individual steps:
```bash
python3 src/01_load_raw.py --raw_dir data/raw --db_path outputs/warehouse.duckdb
python3 src/02_clean.py
python3 src/03_run_sql.py --sql_file sql/06_build_analytics.sql
python3 src/04_feature_engineering.py
python3 src/05_churn_model.py
python3 src/06_ab_testing.py
python3 src/07_export.py
```

---

## Project Structure

```
05_project5_growth_analytics_product_intelligence/
├── data/
│   ├── raw/                   # Olist CSVs (gitignored — download from Kaggle)
│   └── processed/             # Exported CSVs and Parquet files
├── models/
│   └── churn_model.pkl        # Trained GradientBoostingClassifier pipeline
├── notebooks/
│   └── exploration.ipynb      # EDA scratch pad
├── outputs/
│   ├── warehouse.duckdb       # DuckDB warehouse (gitignored)
│   ├── confusion_matrix_*.png
│   ├── feature_importance_*.png
│   └── metrics_comparison.csv
├── app/
│   └── streamlit_app.py       # 4-page interactive app
├── sql/
│   ├── 03_build_dims.sql      # dim.* layer
│   ├── 04_build_fact_orders.sql
│   ├── 05_marketing_spend_and_marts.sql
│   └── 06_build_analytics.sql # analytics.* + extended mart.*
├── src/
│   ├── 01_load_raw.py
│   ├── 02_clean.py
│   ├── 03_run_sql.py
│   ├── 04_feature_engineering.py
│   ├── 05_churn_model.py
│   ├── 06_ab_testing.py
│   └── 07_export.py
├── run_all.py
├── requirements.txt
└── README.md
```

---

## Key Metrics and Business Interpretations

### Customer Lifetime Value (LTV)
LTV milestones measure cumulative revenue per customer at fixed intervals post first purchase. The `predicted_ltv_12m` column extrapolates monthly spend over a 12-month horizon. Single-order customers use their first order value as a conservative lower bound.

### LTV : CAC Ratio
The ratio of median LTV to blended CAC. Target: **≥ 3.0x**. Below 1.0x means the business loses money on every customer acquired.
> Note: CAC is modelled from simulated marketing spend (no real ad platform data in the Olist dataset). Treat as a unit-economics demonstration.

### Cohort Retention
The percentage of customers from a given acquisition cohort who made at least one purchase N months later. Rapid drop-off = high churn; gradual decline = loyal behaviour.

### RFM Segments
| Segment | Description | Action |
|---|---|---|
| champions | High R, F, and M | Reward and upsell |
| loyal | Strong R + F | Early access offers |
| at_risk | Previously active, now quiet | Win-back campaigns |
| cant_lose | High F but lapsed | Urgent reactivation |
| lost | Low R + F | Cost-effective suppression |

### Churn Prediction
Binary classification (churned = no purchase in last 90 days of the dataset window). Model is tuned for ROC-AUC. **Churn probability scores** are used to prioritise retention spend — targeting the top decile by probability is more cost-efficient than blanket campaigns.

### A/B Test Interpretation
- **Chi-squared test** on conversion counts: detects whether the price change affected the proportion of users who placed an order.
- **Welch's t-test** on AOV: detects whether converting users spent more or less on average.
- **Cohen's d** quantifies practical significance independent of sample size.
- A statistically significant result with a negligible Cohen's d has limited business relevance.

---

## A Note on Olist Repeat Purchase Rates

The Olist dataset is a **marketplace order fulfilment dataset**, not a subscription or direct-to-consumer ecommerce store. Approximately **97% of customers make exactly one purchase**, which produces:

- Cohort retention curves that drop to near-zero after Period 0 (acquisition month)
- Extreme class imbalance in churn labels (~90%+ churned under any lookback window)
- `inter_purchase_interval_avg = -1` sentinel for most customers (no second order to measure)

This is **analytically correct** and not a data quality issue. The churn model therefore reflects a realistic high-churn, single-transaction environment. Models are trained with `class_weight='balanced'` and evaluated on ROC-AUC rather than accuracy to account for this imbalance.

---

## Model Performance

After running the pipeline, check `outputs/metrics_comparison.csv` for test set results:

| Model | ROC-AUC | Precision | Recall | F1 |
|---|---|---|---|---|
| Gradient Boosting | — | — | — | — |
| Random Forest     | — | — | — | — |

_Populated after running `python3 run_all.py`._

---

## A/B Testing Methodology

- **Assignment**: Deterministic MD5 hash of `customer_unique_id` with a fixed salt (`pricing_v1`). This ensures stable group membership across pipeline re-runs without requiring a random seed.
- **Treatment**: +10% price increase on `item_price` for the electronics and computers_accessories categories. Freight costs are unchanged (simulates a realistic pricing test where shipping is separately priced).
- **Tests**: Two-sided hypothesis tests at α = 0.05.
- **Retrospective caveat**: This experiment is applied to historical transaction data to illustrate the analytical framework. Results should be interpreted as illustrative, not as causal evidence of pricing elasticity.
