# Project 2 – Campaign Profit Diagnostics

## Objective

Analyze campaign-level marketing data to determine:

- CAC (Customer Acquisition Cost)
- ROAS (Return on Ad Spend)
- Break-even CAC thresholds
- Scaling feasibility and campaign risk classification

---

## Dataset

Ad platform campaign data, included in this folder:

- `campaigns.csv` – campaign budgets and durations
- `ads.csv` – ad-level metadata (platform, type)
- `ad_events.csv` – impression / click / purchase events
- `users.csv` – user demographics

---

## Key Calculations

- Funnel aggregation by campaign (impressions → clicks → purchases)
- CAC = spend / conversions
- ROAS = revenue / spend
- Contribution margin after marketing cost
- Break-even CAC threshold comparison
- Risk classification (profitable / at-risk / margin-negative)

---

## Output

`Profit_Campaign_Risk_Report_v2.xlsx` – executive summary, full campaign
classification, danger campaigns, and profitable campaigns.

---

## How to Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python campaign_audit.py
```
