from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

EXPORTS = {
    "dim_dim_date": "SELECT * FROM dim.dim_date",
    "dim_dim_customers": "SELECT * FROM dim.dim_customers",
    "dim_dim_products": "SELECT * FROM dim.dim_products",
    "dim_dim_channels": "SELECT * FROM dim.dim_channels",
    "dim_dim_cogs_rates": "SELECT * FROM dim.dim_cogs_rates",
    "fct_fct_order_items": "SELECT * FROM fct.fct_order_items",
    "fct_fct_marketing_spend": "SELECT * FROM fct.fct_marketing_spend",
    "mart_daily_exec": "SELECT * FROM mart.mart_daily_exec",
    "mart_product_exec": "SELECT * FROM mart.mart_product_exec",
    "mart_channel_exec": "SELECT * FROM mart.mart_channel_exec",
    "mart_customer_exec": "SELECT * FROM mart.mart_customer_exec",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb")
    ap.add_argument("--out_dir", default="data/processed")
    args = ap.parse_args()

    db_path = Path(args.db_path).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    for name, query in EXPORTS.items():
        out_path = out_dir / f"{name}.csv"

        con.execute(
            f"COPY ({query}) TO '{out_path.as_posix()}' (HEADER, DELIMITER ',');"
        )

        cnt = con.execute(
            f"SELECT COUNT(*) FROM ({query}) t;"
        ).fetchone()[0]

        print(f"Exported {name}: {cnt:,} rows -> {out_path}")

    con.close()


if __name__ == "__main__":
    main()