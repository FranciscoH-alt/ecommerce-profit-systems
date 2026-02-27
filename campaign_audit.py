import pandas as pd
import numpy as np

# ── LOAD DATA ──────────────────────────────────────────────
campaigns = pd.read_csv('campaigns.csv')
ads = pd.read_csv('ads.csv')
users = pd.read_csv('users.csv')
events = pd.read_csv('ad_events.csv')

# ── MERGE ──────────────────────────────────────────────────
df = events.merge(ads[['ad_id', 'campaign_id', 'ad_platform', 'ad_type']],
                  on='ad_id', how='left')
df = df.merge(campaigns[['campaign_id', 'name', 'total_budget',
                          'duration_days', 'start_date', 'end_date']],
              on='campaign_id', how='left')
df = df.merge(users[['user_id', 'user_gender', 'age_group', 'country']],
              on='user_id', how='left')

print(f"Total events loaded: {len(df):,}")
print(f"Event types:\n{df['event_type'].value_counts()}")
print(f"Unique campaigns: {df['campaign_id'].nunique()}")

# ── FUNNEL BY CAMPAIGN (not split by platform) ─────────────
# Group by campaign_id only so budget isn't double counted
funnel = df.groupby(['campaign_id', 'name', 'total_budget', 'duration_days']).agg(
    impressions=('event_type', lambda x: (x == 'Impression').sum()),
    clicks=('event_type', lambda x: (x == 'Click').sum()),
    purchases=('event_type', lambda x: (x == 'Purchase').sum()),
    facebook_impressions=('event_type', lambda x: (
        (x == 'Impression') & (df.loc[x.index, 'ad_platform'] == 'Facebook')).sum()),
    instagram_impressions=('event_type', lambda x: (
        (x == 'Impression') & (df.loc[x.index, 'ad_platform'] == 'Instagram')).sum()),
).reset_index()

# ── FUNNEL RATES ───────────────────────────────────────────
funnel['ctr'] = (funnel['clicks'] / funnel['impressions'] * 100).round(2)
funnel['conversion_rate'] = (funnel['purchases'] / funnel['clicks'] * 100).round(2)
funnel['purchase_rate'] = (funnel['purchases'] / funnel['impressions'] * 100).round(4)

# ── REALISTIC CAC CALCULATION ──────────────────────────────
# Daily budget = total budget spread across campaign duration
funnel['daily_budget'] = funnel['total_budget'] / funnel['duration_days']

# Daily purchases = total purchases spread across campaign duration  
funnel['daily_purchases'] = funnel['purchases'] / funnel['duration_days']

# Monthly equivalents for standardized comparison
funnel['monthly_spend'] = funnel['daily_budget'] * 30
funnel['monthly_purchases'] = funnel['daily_purchases'] * 30

# CAC = monthly spend / monthly purchases
funnel['cac'] = (funnel['monthly_spend'] /
                 funnel['monthly_purchases'].replace(0, np.nan)).round(2)

# ── AOV AND CONTRIBUTION MARGIN ────────────────────────────
# Simulated AOV - in real engagement this comes from Shopify
np.random.seed(42)
base_aov = 2800  # realistic for a gift/retail brand
funnel['avg_order_value'] = base_aov * np.random.uniform(0.85, 1.25, len(funnel))

# Contribution margin % from Project 1 findings
cm_pct = 0.30

# Revenue and margin calculations
funnel['monthly_revenue'] = (funnel['monthly_purchases'] * funnel['avg_order_value']).round(2)
funnel['monthly_cm'] = (funnel['monthly_revenue'] * cm_pct).round(2)
funnel['monthly_profit'] = (funnel['monthly_cm'] - funnel['monthly_spend']).round(2)

# ROAS = revenue / spend
funnel['roas'] = (funnel['monthly_revenue'] / funnel['monthly_spend']).round(2)

# Thresholds
funnel['breakeven_roas'] = round(1 / cm_pct, 2)  # 3.33x
funnel['breakeven_cac'] = (funnel['avg_order_value'] * cm_pct).round(2)

# ── CAMPAIGN RISK CLASSIFIER ───────────────────────────────
def classify_campaign(row):
    if row['purchases'] == 0:
        return 'No Conversions'
    if row['roas'] < row['breakeven_roas'] * 0.85:
        return 'Margin-Negative'
    if row['roas'] < row['breakeven_roas']:
        return 'Break-Even Risk'
    if row['cac'] > row['breakeven_cac'] * 1.2:
        return 'CAC Danger'
    return 'Profitable'

funnel['risk_classification'] = funnel.apply(classify_campaign, axis=1)

# ── PRINT DIAGNOSTICS ──────────────────────────────────────
print("\n--- DIAGNOSTIC: SAMPLE CAMPAIGNS ---")
print(funnel[['name', 'total_budget', 'duration_days', 'purchases',
              'daily_purchases', 'monthly_purchases', 'monthly_spend',
              'cac', 'breakeven_cac', 'roas', 'breakeven_roas']].head(10).to_string())

print("\n--- CAC AND ROAS SUMMARY ---")
print(f"Break-even ROAS threshold: {funnel['breakeven_roas'].iloc[0]:.2f}x")
print(f"Average actual ROAS: {funnel['roas'].mean():.2f}x")
print(f"Average CAC: ${funnel['cac'].mean():.2f}")
print(f"Average break-even CAC: ${funnel['breakeven_cac'].mean():.2f}")

print("\n--- CAMPAIGN RISK CLASSIFICATION ---")
print(funnel['risk_classification'].value_counts())

# ── DOLLAR IMPACT ──────────────────────────────────────────
print("\n--- DOLLAR IMPACT BY CLASSIFICATION ---")
impact = funnel.groupby('risk_classification').agg(
    campaigns=('campaign_id', 'count'),
    total_monthly_spend=('monthly_spend', 'sum'),
    total_monthly_revenue=('monthly_revenue', 'sum'),
    total_monthly_cm=('monthly_cm', 'sum'),
    total_monthly_profit=('monthly_profit', 'sum')
).round(2)
print(impact)

danger_monthly_spend = funnel[funnel['risk_classification'].isin(
    ['Margin-Negative', 'Break-Even Risk', 'CAC Danger'])]['monthly_spend'].sum()
print(f"\nMonthly spend on dangerous campaigns: ${danger_monthly_spend:,.2f}")
print(f"Annualized risk exposure: ${danger_monthly_spend * 12:,.2f}")

# ── EXPORT EXCEL REPORT ────────────────────────────────────
total_monthly_spend = funnel['monthly_spend'].sum()
total_monthly_revenue = funnel['monthly_revenue'].sum()
total_monthly_cm = funnel['monthly_cm'].sum()
total_monthly_profit = funnel['monthly_profit'].sum()
profitable_count = (funnel['risk_classification'] == 'Profitable').sum()
danger_count = funnel[funnel['risk_classification'].isin(
    ['Margin-Negative', 'Break-Even Risk', 'CAC Danger'])].shape[0]

with pd.ExcelWriter('AI_Profit_Campaign_Risk_Report_v2.xlsx', engine='openpyxl') as writer:

    # Tab 1: Executive Summary
    summary = pd.DataFrame({
        'Metric': [
            'Total Campaigns Analyzed',
            'Total Monthly Ad Spend',
            'Total Monthly Estimated Revenue',
            'Total Monthly Contribution Margin',
            'Net Monthly Profit After Ad Spend',
            'Overall ROAS',
            'Break-Even ROAS Threshold',
            'Profitable Campaigns',
            'At-Risk or Margin-Negative Campaigns',
            'Monthly Spend on Dangerous Campaigns',
            'Annualized Risk Exposure'
        ],
        'Value': [
            f"{len(funnel)}",
            f"${total_monthly_spend:,.2f}",
            f"${total_monthly_revenue:,.2f}",
            f"${total_monthly_cm:,.2f}",
            f"${total_monthly_profit:,.2f}",
            f"{(total_monthly_revenue / total_monthly_spend):.2f}x",
            f"{1/cm_pct:.2f}x",
            f"{profitable_count}",
            f"{danger_count}",
            f"${danger_monthly_spend:,.2f}",
            f"${danger_monthly_spend * 12:,.2f}"
        ]
    })
    summary.to_excel(writer, sheet_name='Executive Summary', index=False)

    # Tab 2: Full campaign classification
    campaign_detail = funnel[[
        'campaign_id', 'name', 'total_budget', 'duration_days',
        'impressions', 'clicks', 'purchases', 'ctr', 'conversion_rate',
        'monthly_spend', 'monthly_revenue', 'roas', 'breakeven_roas',
        'cac', 'breakeven_cac', 'monthly_cm', 'monthly_profit',
        'risk_classification'
    ]].sort_values('risk_classification')
    campaign_detail.to_excel(writer, sheet_name='Campaign Classification', index=False)

    # Tab 3: Danger campaigns only
    danger = funnel[funnel['risk_classification'].isin(
        ['Margin-Negative', 'Break-Even Risk', 'CAC Danger'])].sort_values('monthly_profit')
    danger[['name', 'total_budget', 'roas', 'breakeven_roas',
            'cac', 'breakeven_cac', 'monthly_profit',
            'risk_classification']].to_excel(writer, sheet_name='Danger Campaigns', index=False)

    # Tab 4: Profitable campaigns
    profitable = funnel[funnel['risk_classification'] == 'Profitable'].sort_values(
        'monthly_profit', ascending=False)
    profitable[['name', 'total_budget', 'roas', 'cac',
                'breakeven_cac', 'monthly_profit']].to_excel(
        writer, sheet_name='Profitable Campaigns', index=False)

print("\nReport exported: AI_Profit_Campaign_Risk_Report_v2.xlsx")
