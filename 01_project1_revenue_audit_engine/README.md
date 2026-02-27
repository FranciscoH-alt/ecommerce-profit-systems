# Project 1 – Revenue Audit Engine

## Objective

Analyze order-level ecommerce data to identify:

- Revenue inconsistencies
- Margin leakage
- Discount erosion
- Contribution margin behavior

This project simulates a profit audit for ecommerce operators.

---

## Required Dataset

Example: Olist ecommerce dataset

Expected columns (minimum):

- order_id
- customer_id
- order_purchase_timestamp
- price
- freight_value
- payment_value

Place dataset in:


data/


Example:


data/olist_orders_dataset.csv


---

## Key Calculations

- Revenue per order
- Estimated cost inputs
- Contribution margin
- Margin percentage
- Simulated ad spend impact

---

## How to Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python audit.py
