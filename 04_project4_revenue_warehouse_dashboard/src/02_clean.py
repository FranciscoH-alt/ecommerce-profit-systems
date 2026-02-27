from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb")
    args = ap.parse_args()

    db_path = Path(args.db_path).resolve()
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA enable_progress_bar=true;")

    con.execute("CREATE SCHEMA IF NOT EXISTS stg;")

    #Clean orders with typed timestamps
    con.execute("DROP TABLE IF EXISTS stg.orders;")
    con.execute(
        """
        CREATE TABLE stg.orders AS
        SELECT 
            order_id::VARCHAR AS order_id,
            customer_id::VARCHAR AS customer_id,
            order_status::VARCHAR AS order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
            CAST(order_approved_at AS TIMESTAMP) AS order_approved_ts,
            CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_ts,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
        FROM raw.orders
        WHERE order_id IS NOT NULL AND customer_id IS NOT NULL;
        """
    )
     # Order items
    con.execute("DROP TABLE IF EXISTS stg.order_items;")
    con.execute(
        """
        CREATE TABLE stg.order_items AS
        SELECT
            order_id::VARCHAR AS order_id,
            order_item_id::INTEGER AS order_item_id,
            product_id::VARCHAR AS product_id,
            seller_id::VARCHAR AS seller_id,
            CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
            price::DOUBLE AS item_price,
            freight_value::DOUBLE AS freight_value
        FROM raw.order_items
        WHERE order_id IS NOT NULL AND product_id IS NOT NULL;
        """
    )

    # Payments: multiple rows per order; keep detail + also aggregate later
    con.execute("DROP TABLE IF EXISTS stg.order_payments;")
    con.execute(
        """
        CREATE TABLE stg.order_payments AS
        SELECT
            order_id::VARCHAR AS order_id,
            payment_sequential::INTEGER AS payment_sequential,
            payment_type::VARCHAR AS payment_type,
            payment_installments::INTEGER AS payment_installments,
            payment_value::DOUBLE AS payment_value
        FROM raw.order_payments
        WHERE order_id IS NOT NULL;
        """
    )

    # Customers
    con.execute("DROP TABLE IF EXISTS stg.customers;")
    con.execute(
        """
        CREATE TABLE stg.customers AS
        SELECT
            customer_id::VARCHAR AS customer_id,
            customer_unique_id::VARCHAR AS customer_unique_id,
            customer_zip_code_prefix::VARCHAR AS customer_zip_code_prefix,
            customer_city::VARCHAR AS customer_city,
            customer_state::VARCHAR AS customer_state
        FROM raw.customers
        WHERE customer_id IS NOT NULL;
        """
    )

    # Products + category translation
    con.execute("DROP TABLE IF EXISTS stg.products;")
    con.execute(
        """
        CREATE TABLE stg.products AS
        SELECT
            product_id::VARCHAR AS product_id,
            product_category_name::VARCHAR AS product_category_name_pt,
            product_name_lenght::INTEGER AS product_name_length,
            product_description_lenght::INTEGER AS product_description_length,
            product_photos_qty::INTEGER AS product_photos_qty,
            product_weight_g::DOUBLE AS product_weight_g,
            product_length_cm::DOUBLE AS product_length_cm,
            product_height_cm::DOUBLE AS product_height_cm,
            product_width_cm::DOUBLE AS product_width_cm
        FROM raw.products
        WHERE product_id IS NOT NULL;
        """
    )

    con.execute("DROP TABLE IF EXISTS stg.category_translation;")
    con.execute(
        """
        CREATE TABLE stg.category_translation AS
        SELECT
            product_category_name::VARCHAR AS product_category_name_pt,
            product_category_name_english::VARCHAR AS product_category_name_en
        FROM raw.category_translation;
        """
    )

    # Data quality assertions: timestamps must exist for purchasable orders
    bad_ts = con.execute(
        """
        SELECT COUNT(*)
        FROM stg.orders
        WHERE order_purchase_ts IS NULL;
        """
    ).fetchone()[0]
    if bad_ts > 0:
        # hard fail: purchase timestamp is required for date dimension / facts
        raise SystemExit(f"Data quality fail: {bad_ts:,} orders have NULL order_purchase_ts")

    print("OK: staging tables built in schema stg.*")
    con.close()

if __name__ == "__main__":
    main()