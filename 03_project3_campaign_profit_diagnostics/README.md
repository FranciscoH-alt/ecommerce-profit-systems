# Project 3 – Campaign Profit Diagnostics

## Objective

Analyze campaign-level marketing data to determine:

- CAC (Customer Acquisition Cost)
- ROAS (Return on Ad Spend)
- Break-even CAC thresholds
- Scaling feasibility

---

## Required Dataset

Campaign-level dataset with:

- campaign_name
- spend
- conversions
- revenue

Place inside: data/

## Key Calculations

- CAC = spend / conversions
- ROAS = revenue / spend
- Contribution margin after marketing cost
- Break-even threshold comparison

---

## How to Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python campaign_analysis.py
