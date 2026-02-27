import pandas as pd
import numpy as np

# Load each file
orders = pd.read_csv('olist_orders_dataset.csv')
items = pd.read_csv('olist_order_items_dataset.csv')
payments = pd.read_csv('olist_order_payments_dataset.csv')
products = pd.read_csv('olist_products_dataset.csv')
customers = pd.read_csv('olist_customers_dataset.csv')

# Keep only delivered orders (same as filtering completed Shopify orders)
orders = orders[orders['order_status'] == 'delivered']

# Merge orders with items (connects each order to its products)
df = orders.merge(items, on='order_id', how='left')

# Merge with payments (connects each order to payment amount)
payments_agg = payments.groupby('order_id')['payment_value'].sum().reset_index()
df = df.merge(payments_agg, on='order_id', how='left')

# Merge with products (connects product category info)
df = df.merge(products[['product_id','product_category_name']], on='product_id', how='left')

# Merge with customers (connects customer location)
df = df.merge(customers[['customer_id','customer_state']], on='customer_id', how='left')

print(df.shape)
print(df.head())



# Set random seed so results are consistent every time you run
np.random.seed(42)

# COGS: cost of goods sold per item
# Real clients give you this from their supplier invoices
# We simulate it as 35-50% of the sale price
df['cogs'] = df['price'] * np.random.uniform(0.35, 0.50, len(df))

# Shipping cost per order
# Real clients get this from their shipping provider (ShipBob, USPS etc)
df['shipping_cost'] = df['freight_value']

# Payment processing fee (Stripe/Shopify Payments charge ~2.9% + $0.30)
df['payment_fee'] = (df['payment_value'] * 0.029) + 0.30

# Return rate simulation
# Real clients give you this from Shopify returns data
# We simulate 8% of orders as returned
df['is_returned'] = np.random.choice([0, 1], size=len(df), p=[0.92, 0.08])
df['return_cost'] = df['is_returned'] * df['price'] * 0.15  # return processing cost

# Ad spend attribution per order
# Real clients connect Meta Ads and Google Ads
# We simulate three channels with realistic CAC ranges
channels = ['Meta Ads', 'Google Ads', 'Organic']
df['acquisition_channel'] = np.random.choice(channels, size=len(df), p=[0.45, 0.30, 0.25])

# Ad cost per order by channel
# Meta is typically more expensive than Google for e-commerce
ad_cost_map = {'Meta Ads': 18, 'Google Ads': 12, 'Organic': 0}
df['ad_spend_per_order'] = df['acquisition_channel'].map(ad_cost_map)
# Add realistic variance so not every order costs exactly the same
df['ad_spend_per_order'] = df['ad_spend_per_order'] * np.random.uniform(0.5, 2.2, len(df))


# Revenue per order (what the customer paid)
df['revenue'] = df['payment_value']

# Contribution Margin formula:
# Revenue - COGS - Shipping - Payment Fee - Return Cost - Ad Spend
df['contribution_margin'] = (
    df['revenue']
    - df['cogs']
    - df['shipping_cost']
    - df['payment_fee']
    - df['return_cost']
    - df['ad_spend_per_order']
)

# Contribution Margin % (what % of each dollar is actual profit)
df['cm_percentage'] = (df['contribution_margin'] / df['revenue']) * 100

# Flag orders where contribution margin is negative
# These are orders where the brand LOST money
df['is_margin_negative'] = df['contribution_margin'] < 0

print("\n--- CONTRIBUTION MARGIN SUMMARY ---")
print(f"Total Orders Analyzed: {len(df):,}")
print(f"Average CM per Order: ${df['contribution_margin'].mean():.2f}")
print(f"Average CM %: {df['cm_percentage'].mean():.1f}%")
print(f"Margin-Negative Orders: {df['is_margin_negative'].sum():,} ({df['is_margin_negative'].mean()*100:.1f}%)")



# FINDING 1: Channel-level CAC analysis
print("\n--- FINDING 1: CHANNEL PERFORMANCE ---")
channel_analysis = df.groupby('acquisition_channel').agg(
    total_orders=('order_id', 'count'),
    avg_revenue=('revenue', 'mean'),
    avg_cogs=('cogs', 'mean'),
    avg_ad_spend=('ad_spend_per_order', 'mean'),
    avg_cm=('contribution_margin', 'mean'),
    avg_cm_pct=('cm_percentage', 'mean'),
    negative_margin_orders=('is_margin_negative', 'sum'),
    total_cm=('contribution_margin', 'sum')
).round(2)

# Break-even CAC per channel
# This is the maximum you can spend to acquire a customer before losing money
channel_analysis['breakeven_cac'] = (
    df.groupby('acquisition_channel')['revenue'].mean() -
    df.groupby('acquisition_channel')['cogs'].mean() -
    df.groupby('acquisition_channel')['shipping_cost'].mean() -
    df.groupby('acquisition_channel')['payment_fee'].mean()
)

print(channel_analysis)

# FINDING 2: SKU-level profitability
print("\n--- FINDING 2: CATEGORY PROFITABILITY ---")
sku_analysis = df.groupby('product_category_name').agg(
    total_orders=('order_id', 'count'),
    avg_revenue=('revenue', 'mean'),
    avg_cm=('contribution_margin', 'mean'),
    avg_cm_pct=('cm_percentage', 'mean'),
    total_cm=('contribution_margin', 'sum'),
    negative_orders=('is_margin_negative', 'sum')
).round(2).sort_values('avg_cm_pct', ascending=True)

# Bottom 10 worst performing categories
print("WORST 10 CATEGORIES BY MARGIN %:")
print(sku_analysis.head(10))

# FINDING 3: Annualized leakage calculation
print("\n--- FINDING 3: ANNUALIZED LEAKAGE ---")

# Leakage from margin-negative orders
negative_orders = df[df['is_margin_negative'] == True]
monthly_negative_loss = abs(negative_orders['contribution_margin'].sum()) / 12
annualized_leakage = monthly_negative_loss * 12

# Leakage from underperforming channels
# Any channel with CM% below 15% is considered underperforming
underperforming = df[df['cm_percentage'] < 15]
channel_leakage = abs(underperforming['contribution_margin'].sum())

total_leakage = annualized_leakage + (channel_leakage * 0.5)

print(f"Loss from margin-negative orders (annualized): ${annualized_leakage:,.0f}")
print(f"Estimated channel inefficiency leakage: ${channel_leakage:,.0f}")
print(f"TOTAL IDENTIFIED LEAKAGE: ${total_leakage:,.0f}")



# Create Excel report with multiple tabs
with pd.ExcelWriter('AI_Profit_Margin_Audit_Report.xlsx', engine='openpyxl') as writer:
    
    # Tab 1: Executive Summary
    summary_data = {
        'Metric': [
            'Total Orders Analyzed',
            'Average Revenue Per Order',
            'Average Contribution Margin Per Order',
            'Average CM %',
            'Margin-Negative Orders',
            'Margin-Negative Order Rate',
            'Total Identified Annual Leakage'
        ],
        'Value': [
            f"{len(df):,}",
            f"${df['revenue'].mean():.2f}",
            f"${df['contribution_margin'].mean():.2f}",
            f"{df['cm_percentage'].mean():.1f}%",
            f"{df['is_margin_negative'].sum():,}",
            f"{df['is_margin_negative'].mean()*100:.1f}%",
            f"${total_leakage:,.0f}"
        ]
    }
    pd.DataFrame(summary_data).to_excel(writer, sheet_name='Executive Summary', index=False)
    
    # Tab 2: Order-level detail
    order_detail = df[[
        'order_id', 'acquisition_channel', 'product_category_name',
        'revenue', 'cogs', 'shipping_cost', 'payment_fee',
        'return_cost', 'ad_spend_per_order', 'contribution_margin',
        'cm_percentage', 'is_margin_negative'
    ]].round(2)
    order_detail.to_excel(writer, sheet_name='Order Detail', index=False)
    
    # Tab 3: Channel analysis
    channel_analysis.to_excel(writer, sheet_name='Channel Analysis')
    
    # Tab 4: SKU/Category analysis
    sku_analysis.to_excel(writer, sheet_name='Category Analysis')
    
    # Tab 5: Margin-negative orders only
    negative_orders[[
        'order_id', 'acquisition_channel', 'product_category_name',
        'revenue', 'contribution_margin', 'cm_percentage'
    ]].round(2).to_excel(writer, sheet_name='Margin Negative Orders', index=False)

print("\nReport exported: AI_Profit_Margin_Audit_Report.xlsx")