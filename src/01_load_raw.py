from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

RAW_TABLES = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "category_translation",
}

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw_dir", default="data/raw")
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir).resolve()
    db_path = Path(args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA enable_progress_bar=true;")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    missing = []
    for fname, tname in RAW_TABLES.items():
        fpath = raw_dir / fname
        if not fpath.exists():
            missing.append(fname)
            continue
        con.execute(f"DROP TABLE IF EXISTS raw.{tname};")
        # read_csv_auto is robust and fast
        con.execute(
            f"""
            CREATE TABLE raw.{tname} AS
            SELECT * FROM read_csv_auto('{fpath.as_posix()}', HEADER=TRUE, SAMPLE_SIZE=-1);
            """
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM raw.{tname};").fetchone()[0]
        print(f"Loaded raw.{tname}: {cnt:,} rows")

    if missing:
        raise SystemExit(f"Missing files in {raw_dir}: {missing}")

    required = ["orders", "order_items", "order_payments", "customers", "products"]
    for t in required:
        cnt = con.execute(f"SELECT COUNT(*) FROM raw.{t};").fetchone()[0]
        if cnt == 0:
            raise SystemExit(f"Table raw.{t} is empty. Fix your raw data.")

    con.close()
    print(f"\nOK: DuckDB warehouse at {db_path}")

if __name__ == "__main__":
    main()