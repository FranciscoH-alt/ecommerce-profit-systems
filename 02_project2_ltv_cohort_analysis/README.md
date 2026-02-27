# Project 2 – LTV Cohort Analysis

## Objective

Model customer retention and lifetime value using cohort analysis.

Identifies:

- Monthly cohort retention
- Repeat purchase patterns
- Revenue per customer over time
- Estimated LTV behavior

---

## Required Dataset

Example: Online Retail II dataset

Required columns:

- InvoiceNo
- CustomerID
- InvoiceDate
- Quantity
- UnitPrice

Place dataset inside: data/

## Key Calculations

- Cohort month assignment
- Months since first purchase
- Cohort retention matrix
- Cumulative revenue per cohort

---

## How to Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python ltv_cohort.py
