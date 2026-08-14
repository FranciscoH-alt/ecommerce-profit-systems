"""
02_clean.py
───────────
Build the stg.* staging layer from raw.* tables.
Applies type casting, null filtering, and data quality assertions.

Usage:
    python3 src/02_clean.py --db_path outputs/warehouse.duckdb
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser(description="Build DuckDB staging layer from raw tables.")
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb", help="Path to DuckDB warehouse file")
    args = ap.parse_args()

    db_path = Path(args.db_path).resolve()
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA enable_progress_bar=true;")
    con.execute("CREATE SCHEMA IF NOT EXISTS stg;")

    # ── orders ──────────────────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.orders;")
    con.execute("""
        CREATE TABLE stg.orders AS
        SELECT
            order_id::VARCHAR                            AS order_id,
            customer_id::VARCHAR                         AS customer_id,
            order_status::VARCHAR                        AS order_status,
            CAST(order_purchase_timestamp  AS TIMESTAMP) AS order_purchase_ts,
            CAST(order_approved_at         AS TIMESTAMP) AS order_approved_ts,
            CAST(order_delivered_carrier_date  AS TIMESTAMP) AS order_delivered_carrier_ts,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
        FROM raw.orders
        WHERE order_id IS NOT NULL AND customer_id IS NOT NULL;
    """)
    print(f"  stg.orders:          {con.execute('SELECT COUNT(*) FROM stg.orders').fetchone()[0]:>9,} rows")

    # ── order_items ──────────────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.order_items;")
    con.execute("""
        CREATE TABLE stg.order_items AS
        SELECT
            order_id::VARCHAR                     AS order_id,
            order_item_id::INTEGER                AS order_item_id,
            product_id::VARCHAR                   AS product_id,
            seller_id::VARCHAR                    AS seller_id,
            CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
            price::DOUBLE                          AS item_price,
            freight_value::DOUBLE                  AS freight_value
        FROM raw.order_items
        WHERE order_id IS NOT NULL AND product_id IS NOT NULL;
    """)
    print(f"  stg.order_items:     {con.execute('SELECT COUNT(*) FROM stg.order_items').fetchone()[0]:>9,} rows")

    # ── order_payments ───────────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.order_payments;")
    con.execute("""
        CREATE TABLE stg.order_payments AS
        SELECT
            order_id::VARCHAR              AS order_id,
            payment_sequential::INTEGER    AS payment_sequential,
            payment_type::VARCHAR          AS payment_type,
            payment_installments::INTEGER  AS payment_installments,
            payment_value::DOUBLE          AS payment_value
        FROM raw.order_payments
        WHERE order_id IS NOT NULL;
    """)
    print(f"  stg.order_payments:  {con.execute('SELECT COUNT(*) FROM stg.order_payments').fetchone()[0]:>9,} rows")

    # ── customers ────────────────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.customers;")
    con.execute("""
        CREATE TABLE stg.customers AS
        SELECT
            customer_id::VARCHAR             AS customer_id,
            customer_unique_id::VARCHAR      AS customer_unique_id,
            customer_zip_code_prefix::VARCHAR AS customer_zip_code_prefix,
            customer_city::VARCHAR           AS customer_city,
            customer_state::VARCHAR          AS customer_state
        FROM raw.customers
        WHERE customer_id IS NOT NULL;
    """)
    print(f"  stg.customers:       {con.execute('SELECT COUNT(*) FROM stg.customers').fetchone()[0]:>9,} rows")

    # ── products ─────────────────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.products;")
    con.execute("""
        CREATE TABLE stg.products AS
        SELECT
            product_id::VARCHAR                  AS product_id,
            product_category_name::VARCHAR        AS product_category_name_pt,
            product_name_lenght::INTEGER          AS product_name_length,
            product_description_lenght::INTEGER   AS product_description_length,
            product_photos_qty::INTEGER           AS product_photos_qty,
            product_weight_g::DOUBLE              AS product_weight_g,
            product_length_cm::DOUBLE             AS product_length_cm,
            product_height_cm::DOUBLE             AS product_height_cm,
            product_width_cm::DOUBLE              AS product_width_cm
        FROM raw.products
        WHERE product_id IS NOT NULL;
    """)
    print(f"  stg.products:        {con.execute('SELECT COUNT(*) FROM stg.products').fetchone()[0]:>9,} rows")

    # ── category_translation ─────────────────────────────────────────────────
    con.execute("DROP TABLE IF EXISTS stg.category_translation;")
    con.execute("""
        CREATE TABLE stg.category_translation AS
        SELECT
            product_category_name::VARCHAR         AS product_category_name_pt,
            product_category_name_english::VARCHAR AS product_category_name_en
        FROM raw.category_translation;
    """)
    print(f"  stg.category_translation: {con.execute('SELECT COUNT(*) FROM stg.category_translation').fetchone()[0]:>5,} rows")

    # ── data quality assertions ───────────────────────────────────────────────
    bad_ts = con.execute(
        "SELECT COUNT(*) FROM stg.orders WHERE order_purchase_ts IS NULL;"
    ).fetchone()[0]
    if bad_ts > 0:
        raise SystemExit(f"FAIL: {bad_ts:,} orders have NULL order_purchase_ts")

    con.close()
    print("\nOK: stg.* staging layer built and validated.")


if __name__ == "__main__":
    main()
