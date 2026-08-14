"""
run_all.py
──────────
Master orchestrator for the Ecommerce Growth Analytics & Product Intelligence Platform.

Executes the full pipeline in order:
  Step 1  — Load raw CSVs → DuckDB raw.*
  Step 2  — Clean & type  → DuckDB stg.*
  Step 3  — Build dimensions (dim.*)
  Step 4  — Build fact tables (fct.*)
  Step 5  — Build marketing spend + P4-compatible mart views
  Step 6  — Build analytics layer (analytics.* + extended mart.*)
  Step 7  — Feature engineering → features.parquet
  Step 8  — Train churn model   → churn_model.pkl + DuckDB predictions
  Step 9  — A/B testing analysis → ab_test_report.csv + ab_test_stats.csv
  Step 10 — Export all tables   → data/processed/*.csv + *.parquet

Usage:
    python3 run_all.py

    # Custom paths:
    python3 run_all.py --raw_dir data/raw --db_path outputs/warehouse.duckdb --out_dir data/processed
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> None:
    print("\n" + "─" * 60)
    print("▶  " + " ".join(str(c) for c in cmd))
    print("─" * 60)
    subprocess.check_call([str(c) for c in cmd], cwd=str(ROOT))


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the full Growth Analytics pipeline.")
    ap.add_argument("--raw_dir",    default="data/raw",               help="Olist CSV directory")
    ap.add_argument("--db_path",    default="outputs/warehouse.duckdb", help="DuckDB warehouse path")
    ap.add_argument("--out_dir",    default="data/processed",          help="Export output directory")
    ap.add_argument("--models_dir", default="models",                  help="Model artifact directory")
    args = ap.parse_args()

    py = sys.executable
    DB  = args.db_path
    RAW = args.raw_dir
    OUT = args.out_dir
    MDL = args.models_dir
    FTR = f"{OUT}/features.parquet"

    # ── Step 1: Ingest raw CSVs ──────────────────────────────────────────────
    run([py, "src/01_load_raw.py", "--raw_dir", RAW, "--db_path", DB])

    # ── Step 2: Stage & clean ────────────────────────────────────────────────
    run([py, "src/02_clean.py", "--db_path", DB])

    # ── Steps 3–6: SQL warehouse layers ─────────────────────────────────────
    for sql_file in [
        "sql/03_build_dims.sql",
        "sql/04_build_fact_orders.sql",
        "sql/05_marketing_spend_and_marts.sql",
        "sql/06_build_analytics.sql",
    ]:
        run([py, "src/03_run_sql.py", "--db_path", DB, "--sql_file", sql_file])

    # ── Step 7: Feature engineering ─────────────────────────────────────────
    run([py, "src/04_feature_engineering.py", "--db_path", DB, "--out_dir", OUT])

    # ── Step 8: Churn model ──────────────────────────────────────────────────
    run([
        py, "src/05_churn_model.py",
        "--db_path",       DB,
        "--features_path", FTR,
        "--out_dir",       "outputs",
        "--models_dir",    MDL,
    ])

    # ── Step 9: A/B testing ──────────────────────────────────────────────────
    run([py, "src/06_ab_testing.py", "--db_path", DB, "--out_dir", OUT])

    # ── Step 10: Export all tables ───────────────────────────────────────────
    run([py, "src/07_export.py", "--db_path", DB, "--out_dir", OUT])

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print(f"  Warehouse  : {DB}")
    print(f"  Exports    : {OUT}/")
    print(f"  Model      : {MDL}/churn_model.pkl")
    print(f"  Charts     : outputs/")
    print("=" * 60)
    print("\nTo launch the Streamlit app:")
    print("  python3 -m streamlit run app/streamlit_app.py")


if __name__ == "__main__":
    main()
