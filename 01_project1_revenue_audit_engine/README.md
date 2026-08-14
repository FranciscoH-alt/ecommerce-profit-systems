# Project 1 – Revenue Audit Engine

## Objective

Analyze order-level ecommerce data to identify:

- Revenue inconsistencies
- Margin leakage
- Discount erosion
- Contribution margin behavior

This project simulates a profit audit for ecommerce operators.

---

## Dataset

Olist ecommerce dataset, included in the `DATATSET/` folder:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`

---

## Key Calculations

- Revenue per order
- Estimated cost inputs
- Contribution margin
- Margin percentage
- Simulated ad spend impact

---

## Output

`Profit_Margin_Audit_Report.xlsx` – executive summary, order detail,
channel analysis, category analysis, and margin-negative orders.

---

## How to Run

```bash
cd DATATSET
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python audit.py
```
