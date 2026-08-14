"""
07_export.py
────────────
Export all analytics.* and mart.* tables from DuckDB to CSV and Parquet.

Outputs are written to data/processed/ and can be consumed by:
  - BI tools (Tableau, Power BI, Metabase)
  - Notebooks / ad-hoc analysis
  - The Streamlit app (falls back to CSV if DuckDB connection unavailable)

Usage:
    python3 src/07_export.py --db_path outputs/warehouse.duckdb --out_dir data/processed
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

# Tables/views to export — (output_name, SQL query)
EXPORTS: dict[str, str] = {
    # Dimension tables
    "dim_date":                  "SELECT * FROM dim.dim_date",
    "dim_customers":             "SELECT * FROM dim.dim_customers",
    "dim_products":              "SELECT * FROM dim.dim_products",
    # Fact tables
    "fct_order_items":           "SELECT * FROM fct.fct_order_items",
    "fct_marketing_spend":       "SELECT * FROM fct.fct_marketing_spend",
    # Analytics layer
    "analytics_customer_ltv":       "SELECT * FROM analytics.customer_ltv",
    "analytics_cohort_retention":   "SELECT * FROM analytics.cohort_retention",
    "analytics_rfm_scores":         "SELECT * FROM analytics.rfm_scores",
    "analytics_repeat_purchase":    "SELECT * FROM analytics.repeat_purchase_behavior",
    "analytics_churn_signals":      "SELECT * FROM analytics.churn_signals",
    "analytics_cac_simulation":     "SELECT * FROM analytics.cac_simulation",
    "analytics_churn_predictions":  "SELECT * FROM analytics.churn_predictions",
    # Mart layer
    "mart_executive_kpis":          "SELECT * FROM mart.executive_kpis",
    "mart_product_performance":     "SELECT * FROM mart.product_performance",
    "mart_customer_health":         "SELECT * FROM mart.customer_health",
    "mart_daily_exec":              "SELECT * FROM mart.mart_daily_exec",
    "mart_product_exec":            "SELECT * FROM mart.mart_product_exec",
    "mart_channel_exec":            "SELECT * FROM mart.mart_channel_exec",
    "mart_customer_exec":           "SELECT * FROM mart.mart_customer_exec",
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Export DuckDB tables to CSV and Parquet.")
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb", help="DuckDB warehouse path")
    ap.add_argument("--out_dir", default="data/processed",           help="Output directory")
    args = ap.parse_args()

    db_path = Path(args.db_path).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)

    total_rows = 0
    for name, query in EXPORTS.items():
        try:
            df = con.execute(query).df()
        except Exception as e:
            print(f"  SKIP {name}: {e}")
            continue

        csv_path     = out_dir / f"{name}.csv"
        parquet_path = out_dir / f"{name}.parquet"

        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)

        total_rows += len(df)
        print(f"  exported {name:<40} {len(df):>9,} rows")

    con.close()
    print(f"\nOK: {len(EXPORTS)} tables exported to {out_dir}  ({total_rows:,} total rows)")


if __name__ == "__main__":
    main()
