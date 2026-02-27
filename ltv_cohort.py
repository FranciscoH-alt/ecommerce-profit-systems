import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ── LOAD DATA ──────────────────────────────────────────────
print("Loading data...")
df = pd.read_excel('online_retail_II.xlsx', sheet_name='Year 2009-2010')
df2 = pd.read_excel('online_retail_II.xlsx', sheet_name='Year 2010-2011')
df = pd.concat([df, df2], ignore_index=True)

print(f"Raw records loaded: {len(df):,}")
print(f"Columns: {df.columns.tolist()}")
print(f"\nSample data:")
print(df.head(3))

# ── CLEAN THE DATA ─────────────────────────────────────────
# Step 1: Remove cancelled orders (Invoice starting with C)
# In Shopify these would be refunded or cancelled orders
df = df[~df['Invoice'].astype(str).str.startswith('C')]

# Step 2: Remove rows with missing Customer ID
# You cannot do cohort analysis without knowing who the customer is
df = df.dropna(subset=['Customer ID'])

# Step 3: Remove negative quantities (returns/adjustments)
df = df[df['Quantity'] > 0]

# Step 4: Remove zero or negative prices
df = df[df['Price'] > 0]

# Step 5: Convert Customer ID to integer
df['Customer ID'] = df['Customer ID'].astype(int)

# Step 6: Parse invoice date
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Step 7: Calculate revenue per line item
df['revenue'] = df['Quantity'] * df['Price']

print(f"\nClean records: {len(df):,}")
print(f"Unique customers: {df['Customer ID'].nunique():,}")
print(f"Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
print(f"Total revenue in dataset: ${df['revenue'].sum():,.2f}")

### BUILD ORDER-LEVEL SUMMARY ###
# ── ORDER LEVEL ────────────────────────────────────────────
# Collapse line items into one row per order
orders = df.groupby(['Invoice', 'Customer ID', 'InvoiceDate', 'Country']).agg(
    order_revenue=('revenue', 'sum'),
    items_purchased=('Quantity', 'sum'),
    unique_products=('StockCode', 'nunique')
).reset_index()

print(f"\nTotal orders: {len(orders):,}")
print(f"Average order value: ${orders['order_revenue'].mean():.2f}")
print(f"Average items per order: {orders['items_purchased'].mean():.1f}")

### BUILD COHORT TABLE ###
# ── COHORT ANALYSIS ────────────────────────────────────────

# Step 1: Find each customer's first purchase date
first_purchase = orders.groupby('Customer ID')['InvoiceDate'].min().reset_index()
first_purchase.columns = ['Customer ID', 'first_purchase_date']
first_purchase['cohort_month'] = first_purchase['first_purchase_date'].dt.to_period('M')

# Step 2: Merge first purchase back to all orders
orders = orders.merge(first_purchase[['Customer ID', 'cohort_month']], 
                      on='Customer ID', how='left')

# Step 3: Calculate months since first purchase for each order
orders['order_month'] = orders['InvoiceDate'].dt.to_period('M')
orders['months_since_first'] = (
    orders['order_month'] - orders['cohort_month']
).apply(lambda x: x.n)

# Step 4: Build cohort revenue table
cohort_revenue = orders.groupby(['cohort_month', 'months_since_first']).agg(
    customers=('Customer ID', 'nunique'),
    revenue=('order_revenue', 'sum')
).reset_index()

# Step 5: Calculate cohort sizes (how many customers in each cohort)
cohort_sizes = cohort_revenue[cohort_revenue['months_since_first'] == 0][
    ['cohort_month', 'customers']].rename(columns={'customers': 'cohort_size'})

cohort_revenue = cohort_revenue.merge(cohort_sizes, on='cohort_month', how='left')

# Step 6: Calculate retention rate per cohort month
cohort_revenue['retention_rate'] = (
    cohort_revenue['customers'] / cohort_revenue['cohort_size'] * 100
).round(1)

# Step 7: Calculate revenue per customer in cohort
cohort_revenue['revenue_per_customer'] = (
    cohort_revenue['revenue'] / cohort_revenue['cohort_size']
).round(2)

print("\n--- COHORT TABLE SAMPLE ---")
print(cohort_revenue[cohort_revenue['months_since_first'] <= 3].head(20))

### CALCULATE LTV
# Now you calculate cumulative LTV at 30, 60, and 90 days for each cohort.
# This is what tells you which acquisition channels are worth scaling.
# ── LTV CALCULATIONS ───────────────────────────────────────

# Cumulative revenue per customer at each month milestone
ltv_summary = cohort_revenue.pivot_table(
    index='cohort_month',
    columns='months_since_first',
    values='revenue_per_customer',
    aggfunc='sum'
).fillna(0)

# Calculate cumulative LTV
ltv_cumulative = ltv_summary.cumsum(axis=1)

# Extract key LTV milestones
ltv_milestones = pd.DataFrame()
ltv_milestones['cohort_month'] = ltv_cumulative.index
ltv_milestones = ltv_milestones.reset_index(drop=True)
ltv_cumulative = ltv_cumulative.reset_index()

# 30-day LTV (month 0 = first purchase month)
if 0 in ltv_cumulative.columns:
    ltv_milestones['ltv_30d'] = ltv_cumulative[0].values

# 60-day LTV (month 1)
if 1 in ltv_cumulative.columns:
    ltv_milestones['ltv_60d'] = ltv_cumulative[1].values

# 90-day LTV (month 2)
if 2 in ltv_cumulative.columns:
    ltv_milestones['ltv_90d'] = ltv_cumulative[2].values

# 180-day LTV (month 5)
if 5 in ltv_cumulative.columns:
    ltv_milestones['ltv_180d'] = ltv_cumulative[5].values

# Merge cohort sizes
ltv_milestones = ltv_milestones.merge(
    cohort_sizes, on='cohort_month', how='left')

print("\n--- LTV BY COHORT ---")
print(ltv_milestones.head(12).to_string())

### BREAK-EVEN CAC PER COHORT
# This is where LTV connects back to your Project 2 work. 
# Now you know how much you can afford to pay to acquire a customer from each cohort.
# ── BREAK-EVEN CAC ─────────────────────────────────────────
cm_pct = 0.30  # contribution margin from Project 1

# Break-even CAC = LTV * CM%
# This is the maximum you can spend to acquire a customer
# and still be profitable over that time period
if 'ltv_30d' in ltv_milestones.columns:
    ltv_milestones['breakeven_cac_30d'] = (ltv_milestones['ltv_30d'] * cm_pct).round(2)
if 'ltv_90d' in ltv_milestones.columns:
    ltv_milestones['breakeven_cac_90d'] = (ltv_milestones['ltv_90d'] * cm_pct).round(2)
if 'ltv_180d' in ltv_milestones.columns:
    ltv_milestones['breakeven_cac_180d'] = (ltv_milestones['ltv_180d'] * cm_pct).round(2)

print("\n--- BREAK-EVEN CAC BY COHORT ---")
cols = ['cohort_month', 'cohort_size', 'ltv_30d', 'ltv_90d', 
        'breakeven_cac_30d', 'breakeven_cac_90d']
available_cols = [c for c in cols if c in ltv_milestones.columns]
print(ltv_milestones[available_cols].head(12).to_string())

### RETENTION ANALYSIS
# Retention tells you how sticky the customer base is. 
# Low retention means the brand is on a treadmill — constantly spending to acquire new customers because old ones never come back.
# ── RETENTION ANALYSIS ─────────────────────────────────────

# Retention pivot table
retention_pivot = cohort_revenue.pivot_table(
    index='cohort_month',
    columns='months_since_first',
    values='retention_rate'
).round(1)

# Average retention by month
avg_retention = cohort_revenue.groupby('months_since_first')['retention_rate'].mean().round(1)

print("\n--- AVERAGE RETENTION BY MONTH ---")
print(avg_retention.head(12))

# Identify best and worst retention cohorts at 90 days
if 2 in retention_pivot.columns:
    retention_90d = retention_pivot[2].dropna().sort_values(ascending=False)
    print(f"\nBest 90-day retention cohort: {retention_90d.index[0]} at {retention_90d.iloc[0]}%")
    print(f"Worst 90-day retention cohort: {retention_90d.index[-1]} at {retention_90d.iloc[-1]}%")


### CUSTOMER SEGMENTATION
# This segments customers into high value, mid value, and low value based on their total spend. 
# In a real engagement this tells the founder which customers to prioritize for retention campaigns in Klaviyo.
# ── CUSTOMER SEGMENTATION ──────────────────────────────────

# Total revenue and orders per customer
customer_summary = orders.groupby('Customer ID').agg(
    total_revenue=('order_revenue', 'sum'),
    total_orders=('Invoice', 'nunique'),
    avg_order_value=('order_revenue', 'mean'),
    first_purchase=('InvoiceDate', 'min'),
    last_purchase=('InvoiceDate', 'max')
).reset_index()

customer_summary['avg_order_value'] = customer_summary['avg_order_value'].round(2)
customer_summary['total_revenue'] = customer_summary['total_revenue'].round(2)

# Customer lifetime in days
customer_summary['customer_lifetime_days'] = (
    customer_summary['last_purchase'] - customer_summary['first_purchase']
).dt.days

# Segment by total revenue
def segment_customer(revenue):
    if revenue >= customer_summary['total_revenue'].quantile(0.75):
        return 'High Value'
    elif revenue >= customer_summary['total_revenue'].quantile(0.25):
        return 'Mid Value'
    else:
        return 'Low Value'

customer_summary['segment'] = customer_summary['total_revenue'].apply(segment_customer)

print("\n--- CUSTOMER SEGMENTATION ---")
segment_stats = customer_summary.groupby('segment').agg(
    customers=('Customer ID', 'count'),
    avg_revenue=('total_revenue', 'mean'),
    avg_orders=('total_orders', 'mean'),
    total_revenue=('total_revenue', 'sum')
).round(2)
print(segment_stats)

### SAFE SCALING THRESHOLDS
# This is the output that directly informs scaling decisions. 
# It answers: given what we know about LTV, what is the maximum safe CAC per channel?
# ── SCALING THRESHOLDS ─────────────────────────────────────

avg_ltv_30d = ltv_milestones['ltv_30d'].mean() if 'ltv_30d' in ltv_milestones.columns else 0
avg_ltv_90d = ltv_milestones['ltv_90d'].mean() if 'ltv_90d' in ltv_milestones.columns else 0
avg_ltv_180d = ltv_milestones['ltv_180d'].mean() if 'ltv_180d' in ltv_milestones.columns else 0

avg_retention_month1 = avg_retention.get(1, 0)
avg_retention_month2 = avg_retention.get(2, 0)

print("\n--- SAFE SCALING THRESHOLDS ---")
print(f"Average 30-day LTV: ${avg_ltv_30d:.2f}")
print(f"Average 90-day LTV: ${avg_ltv_90d:.2f}")
print(f"Average 180-day LTV: ${avg_ltv_180d:.2f}")
print(f"Average Month 1 Retention: {avg_retention_month1:.1f}%")
print(f"Average Month 2 Retention: {avg_retention_month2:.1f}%")
print(f"\nMax safe CAC (30-day payback): ${avg_ltv_30d * cm_pct:.2f}")
print(f"Max safe CAC (90-day payback): ${avg_ltv_90d * cm_pct:.2f}")
print(f"Max safe CAC (180-day payback): ${avg_ltv_180d * cm_pct:.2f}")

### EXPORT CLIENT REPORT
# ── EXPORT EXCEL REPORT ────────────────────────────────────
with pd.ExcelWriter('AI_Profit_LTV_Cohort_Report.xlsx', engine='openpyxl') as writer:

    # Tab 1: Executive Summary
    summary = pd.DataFrame({
        'Metric': [
            'Total Customers Analyzed',
            'Total Orders Analyzed',
            'Total Revenue Analyzed',
            'Average Order Value',
            'Average 30-Day LTV',
            'Average 90-Day LTV',
            'Average 180-Day LTV',
            'Average Month 1 Retention Rate',
            'Average Month 2 Retention Rate',
            'Max Safe CAC (30-day payback)',
            'Max Safe CAC (90-day payback)',
            'Max Safe CAC (180-day payback)'
        ],
        'Value': [
            f"{customer_summary['Customer ID'].nunique():,}",
            f"{len(orders):,}",
            f"${orders['order_revenue'].sum():,.2f}",
            f"${orders['order_revenue'].mean():.2f}",
            f"${avg_ltv_30d:.2f}",
            f"${avg_ltv_90d:.2f}",
            f"${avg_ltv_180d:.2f}",
            f"{avg_retention_month1:.1f}%",
            f"{avg_retention_month2:.1f}%",
            f"${avg_ltv_30d * cm_pct:.2f}",
            f"${avg_ltv_90d * cm_pct:.2f}",
            f"${avg_ltv_180d * cm_pct:.2f}"
        ]
    })
    summary.to_excel(writer, sheet_name='Executive Summary', index=False)

    # Tab 2: LTV by cohort
    ltv_milestones.to_excel(writer, sheet_name='LTV By Cohort', index=False)

    # Tab 3: Retention pivot
    retention_pivot.to_excel(writer, sheet_name='Retention Table')

    # Tab 4: Customer segmentation
    customer_summary.to_excel(writer, sheet_name='Customer Segments', index=False)

    # Tab 5: Cohort detail
    cohort_revenue.to_excel(writer, sheet_name='Cohort Detail', index=False)

print("\nReport exported: AI_Profit_LTV_Cohort_Report.xlsx")
print("\n--- PROJECT 3 COMPLETE ---")
