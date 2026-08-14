# Project 3 – LTV & Cohort Analysis

## Objective

Model customer retention and lifetime value using cohort analysis.

Identifies:

- Monthly cohort retention
- Repeat purchase patterns
- Revenue per customer over time
- Estimated LTV behavior by acquisition cohort

---

## Dataset

Online Retail II dataset, included in this folder as `online_retail_II.xlsx`
(sheets: `Year 2009-2010`, `Year 2010-2011`).

Columns used:

- Invoice
- Customer ID
- InvoiceDate
- Quantity
- Price

---

## Key Calculations

- Cohort month assignment
- Months since first purchase
- Cohort retention matrix
- Cumulative revenue per cohort
- LTV milestones and customer segmentation

---

## Output

`Profit_LTV_Cohort_Report.xlsx` – executive summary, LTV by cohort,
retention table, customer segments, and cohort detail.

---

## How to Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python ltv_cohort.py
```
