"""
01_load_raw.py
──────────────
Ingest all 9 Olist CSVs into DuckDB raw.* schema.
Each table is created with DuckDB's read_csv_auto for zero-config type inference.

Usage:
    python3 src/01_load_raw.py --raw_dir data/raw --db_path outputs/warehouse.duckdb
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

RAW_TABLES: dict[str, str] = {
    "olist_customers_dataset.csv":       "customers",
    "olist_geolocation_dataset.csv":     "geolocation",
    "olist_order_items_dataset.csv":     "order_items",
    "olist_order_payments_dataset.csv":  "order_payments",
    "olist_order_reviews_dataset.csv":   "order_reviews",
    "olist_orders_dataset.csv":          "orders",
    "olist_products_dataset.csv":        "products",
    "olist_sellers_dataset.csv":         "sellers",
    "product_category_name_translation.csv": "category_translation",
}

REQUIRED_TABLES = ["orders", "order_items", "order_payments", "customers", "products"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Load Olist CSVs into DuckDB raw schema.")
    ap.add_argument("--raw_dir",  default="data/raw",               help="Directory containing Olist CSVs")
    ap.add_argument("--db_path",  default="outputs/warehouse.duckdb", help="Path to DuckDB warehouse file")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir).resolve()
    db_path = Path(args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA enable_progress_bar=true;")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    missing: list[str] = []
    for fname, tname in RAW_TABLES.items():
        fpath = raw_dir / fname
        if not fpath.exists():
            missing.append(fname)
            continue
        con.execute(f"DROP TABLE IF EXISTS raw.{tname};")
        con.execute(
            f"""
            CREATE TABLE raw.{tname} AS
            SELECT * FROM read_csv_auto('{fpath.as_posix()}', HEADER=TRUE, SAMPLE_SIZE=-1);
            """
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tname};").fetchone()[0]
        print(f"  loaded raw.{tname:<25} {cnt:>9,} rows")

    if missing:
        raise SystemExit(f"\nMissing CSV files in {raw_dir}:\n  " + "\n  ".join(missing))

    print("\nData quality checks...")
    for t in REQUIRED_TABLES:
        cnt = con.execute(f"SELECT COUNT(*) FROM raw.{t};").fetchone()[0]
        if cnt == 0:
            raise SystemExit(f"FAIL: raw.{t} is empty — check your CSV files.")
        print(f"  raw.{t}: {cnt:,} rows  OK")

    con.close()
    print(f"\nOK: raw schema ready at {db_path}")


if __name__ == "__main__":
    main()
