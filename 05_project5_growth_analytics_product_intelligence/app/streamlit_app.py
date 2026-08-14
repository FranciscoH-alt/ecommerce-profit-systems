"""
streamlit_app.py
────────────────
Ecommerce Growth Analytics & Product Intelligence Platform — Streamlit UI.

Four pages:
  1. Executive KPIs        — monthly revenue, LTV, LTV:CAC ratio, churn/retention
  2. Cohort Retention      — monthly cohort × retention-period heatmap
  3. Churn Risk Explorer   — customer-level churn scores + RFM metadata
  4. A/B Test Results      — pricing experiment metric comparison + significance

Usage:
    streamlit run app/streamlit_app.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import streamlit as st

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[1]
DB_PATH    = ROOT / "outputs" / "warehouse.duckdb"
PROC_DIR   = ROOT / "data" / "processed"
OUTPUTS    = ROOT / "outputs"

# ── DuckDB connection (read-only to avoid pipeline lock conflicts) ─────────
@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        st.error(
            f"Warehouse not found at `{DB_PATH}`.\n\n"
            "Run `python3 run_all.py` first to build the pipeline."
        )
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def query(_con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Cached SQL query helper."""
    try:
        return _con.execute(sql).df()
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 — Executive KPIs
# ─────────────────────────────────────────────────────────────────────────────
def page_executive_kpis(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Executive KPIs")
    st.caption("Monthly performance summary — Revenue, LTV, LTV:CAC, Churn & Retention")

    df = query(con, "SELECT * FROM mart.executive_kpis ORDER BY year_month;")
    if df.empty:
        st.info("No data available. Run the pipeline first.")
        return

    # Drop partial months: Olist ends mid-month, causing the last 1–2 rows to
    # have abnormally low order counts (< 5% of the prior month's average).
    # We keep only months with at least 100 orders so charts aren't skewed.
    df = df[df["orders"] >= 100].copy().reset_index(drop=True)

    if df.empty:
        st.info("No complete months found in the data.")
        return

    latest = df.iloc[-1]

    # For churn / retention use the median of the last 6 complete months so a
    # single edge-cohort (whose period-1 window is cut off by the dataset end)
    # doesn't dominate the headline number.
    recent = df.tail(6)
    avg_churn     = recent["period1_churn_rate"].dropna().median()
    avg_retention = recent["period1_retention_rate"].dropna().median()

    # ── Top metric tiles ────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(
            "Total Revenue (latest month)",
            f"${latest['revenue']:,.0f}",
            delta=f"{latest['revenue_mom_growth']:.1%} MoM"
            if pd.notna(latest["revenue_mom_growth"]) else None,
        )
    with col2:
        median_ltv = df["median_ltv"].dropna().iloc[-1] if df["median_ltv"].notna().any() else None
        st.metric("Median LTV", f"${median_ltv:,.2f}" if median_ltv else "—")
    with col3:
        ltv_cac = df["ltv_cac_ratio"].dropna().iloc[-1] if df["ltv_cac_ratio"].notna().any() else None
        st.metric("LTV : CAC", f"{ltv_cac:.2f}x" if ltv_cac else "—")
    with col4:
        st.metric(
            "Median Period-1 Churn",
            f"{avg_churn:.2%}" if pd.notna(avg_churn) else "—",
            help="Median across the last 6 complete cohort months",
        )
    with col5:
        st.metric(
            "Median Period-1 Retention",
            f"{avg_retention:.2%}" if pd.notna(avg_retention) else "—",
            help="Median across the last 6 complete cohort months",
        )

    st.divider()

    # ── Monthly revenue chart ───────────────────────────────────────────────
    st.subheader("Monthly Revenue")
    rev_df = df[["year_month", "revenue"]].dropna(subset=["revenue"])
    st.line_chart(rev_df.set_index("year_month")["revenue"], use_container_width=True)

    # ── MoM growth chart ────────────────────────────────────────────────────
    st.subheader("Revenue MoM Growth")
    growth_df = df[["year_month", "revenue_mom_growth"]].dropna()
    if not growth_df.empty:
        growth_df = growth_df.copy()
        growth_df["revenue_mom_growth_pct"] = growth_df["revenue_mom_growth"] * 100
        st.bar_chart(growth_df.set_index("year_month")["revenue_mom_growth_pct"], use_container_width=True)

    # ── Detailed table ──────────────────────────────────────────────────────
    st.subheader("Monthly Summary Table")
    display_cols = ["year_month", "revenue", "orders", "customers", "revenue_mom_growth",
                    "median_ltv", "avg_blended_cac", "ltv_cac_ratio",
                    "period1_churn_rate", "period1_retention_rate"]
    available_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[available_cols].sort_values("year_month", ascending=False)
        .style.format({
            "revenue":                "${:,.0f}",
            "median_ltv":             "${:,.2f}",
            "avg_blended_cac":        "${:,.2f}",
            "ltv_cac_ratio":          "{:.2f}x",
            "revenue_mom_growth":     "{:.1%}",
            "period1_churn_rate":     "{:.2%}",
            "period1_retention_rate": "{:.2%}",
        }, na_rep="—"),
        use_container_width=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 — Cohort Retention Heatmap
# ─────────────────────────────────────────────────────────────────────────────
def page_cohort_retention(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Cohort Retention")
    st.caption(
        "Monthly cohort × periods-since-first-purchase retention rates. "
        "Period 0 = acquisition month (always 100%)."
    )

    df = query(con, """
        SELECT cohort_month, periods_since_first, retention_rate, cohort_size
        FROM analytics.cohort_retention
        ORDER BY cohort_month, periods_since_first;
    """)

    if df.empty:
        st.info("No cohort data available.")
        return

    # Exclude the last 2 cohort months: their period-1 window falls within the
    # dataset's cutoff, making their retention look artificially worse (100% churn).
    all_cohorts = sorted(df["cohort_month"].unique())
    cohorts_to_show = all_cohorts[:-2] if len(all_cohorts) > 2 else all_cohorts
    df = df[df["cohort_month"].isin(cohorts_to_show)]

    max_periods = st.slider("Max periods to display", 1, 12, 6)
    df_filtered = df[df["periods_since_first"] <= max_periods]

    # Pivot to matrix (exclude period 0 — always 100%, wastes space)
    pivot = df_filtered[df_filtered["periods_since_first"] >= 1].pivot_table(
        index="cohort_month",
        columns="periods_since_first",
        values="retention_rate",
        aggfunc="mean",
    )
    pivot.columns = [f"M+{int(c)}" for c in pivot.columns]
    pivot.index.name = "Cohort"

    # Scale colour bar to the actual data range (not 0–100%)
    # so the tiny variation in ~0–1% retention is visible
    vmax = float(pivot.max().max()) if not pivot.empty else 0.01
    vmax = max(vmax, 0.005)  # floor so empty pivots don't error

    # Custom annotation: show as "0.52%" not "0%"
    annot_labels = pivot.applymap(
        lambda v: f"{v:.2%}" if pd.notna(v) else ""
    )

    fig, ax = plt.subplots(figsize=(max(10, max_periods + 3), max(6, len(pivot) * 0.45 + 1)))
    sns.heatmap(
        pivot,
        annot=annot_labels,
        fmt="",
        cmap="YlOrRd_r",
        linewidths=0.5,
        linecolor="white",
        vmin=0,
        vmax=vmax,
        ax=ax,
        cbar_kws={"label": "Retention Rate (scaled to data range)"},
    )
    ax.set_title("Monthly Cohort Retention Heatmap (Period 0 = 100%, excluded)", fontsize=13, pad=14)
    ax.set_xlabel("Months Since First Purchase")
    ax.set_ylabel("Acquisition Cohort")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

    # Cohort size bar chart
    st.divider()
    st.subheader("Cohort Sizes (new customers acquired per month)")
    sizes = df[df["periods_since_first"] == 0][["cohort_month", "cohort_size"]].sort_values("cohort_month")
    st.bar_chart(sizes.set_index("cohort_month")["cohort_size"], use_container_width=True)

    st.info(
        "**Note on Olist data:** The Olist dataset reflects a marketplace fulfilment model where "
        "~97% of customers make exactly one purchase. Retention rates after Period 0 range from "
        "0.02% to ~0.6% — this is analytically correct and reflects a high-churn, low-repeat "
        "environment, not a data quality issue."
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 — Churn Risk Explorer
# ─────────────────────────────────────────────────────────────────────────────
def page_churn_explorer(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Churn Risk Explorer")
    st.caption("Customer-level churn probability scores with RFM context.")

    pred_df = query(con, "SELECT * FROM analytics.churn_predictions;")

    if pred_df.empty:
        st.info(
            "No predictions available yet.\n\n"
            "Run `python3 src/05_churn_model.py` (or `python3 run_all.py`) to generate scores."
        )
        return

    rfm_df = query(con, """
        SELECT customer_unique_id, rfm_segment, recency_days, frequency,
               monetary_total, r_score, f_score, m_score
        FROM analytics.rfm_scores;
    """)

    ltv_df = query(con, """
        SELECT customer_unique_id, last_order_date, predicted_ltv_12m, lifetime_orders
        FROM analytics.customer_ltv;
    """)

    merged = pred_df.merge(rfm_df, on="customer_unique_id", how="left")
    merged = merged.merge(ltv_df, on="customer_unique_id", how="left")

    # ── Summary metrics ──────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Scored Customers", f"{len(merged):,}")
    with col2:
        high_risk = (merged["churn_probability"] >= 0.7).sum()
        st.metric("High Risk (p ≥ 0.7)", f"{high_risk:,}", delta=f"{high_risk/len(merged):.1%} of base")
    with col3:
        avg_prob = merged["churn_probability"].mean()
        st.metric("Avg Churn Probability", f"{avg_prob:.1%}")

    st.divider()

    # ── Filters ──────────────────────────────────────────────────────────────
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        threshold = st.slider("Minimum churn probability", 0.0, 1.0, 0.5, step=0.05)
    with col_f2:
        all_segments = ["All"] + sorted(merged["rfm_segment"].dropna().unique().tolist())
        selected_segment = st.selectbox("RFM Segment filter", all_segments)

    filtered = merged[merged["churn_probability"] >= threshold]
    if selected_segment != "All":
        filtered = filtered[filtered["rfm_segment"] == selected_segment]

    st.write(f"Showing **{len(filtered):,}** customers (p ≥ {threshold:.0%})")

    # ── Customer table ────────────────────────────────────────────────────────
    display_cols = [
        "customer_unique_id", "churn_probability", "predicted_churned",
        "rfm_segment", "recency_days", "frequency", "monetary_total",
        "predicted_ltv_12m", "last_order_date",
    ]
    available = [c for c in display_cols if c in filtered.columns]
    st.dataframe(
        filtered[available]
        .sort_values("churn_probability", ascending=False)
        .head(500)
        .style.background_gradient(subset=["churn_probability"], cmap="Reds")
        .format({
            "churn_probability":  "{:.1%}",
            "monetary_total":     "${:,.2f}",
            "predicted_ltv_12m":  "${:,.2f}",
        }, na_rep="—"),
        use_container_width=True,
    )

    # ── RFM segment breakdown of at-risk customers ────────────────────────────
    st.subheader("RFM Segment Breakdown (filtered)")
    seg_counts = filtered["rfm_segment"].value_counts().rename_axis("segment").reset_index(name="count")
    if not seg_counts.empty:
        st.bar_chart(seg_counts.set_index("segment")["count"], use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 4 — A/B Test Results
# ─────────────────────────────────────────────────────────────────────────────
def page_ab_results(con: duckdb.DuckDBPyConnection) -> None:
    st.title("A/B Test Results")
    st.caption(
        "Pricing experiment: +10% price increase on *electronics* and *computers_accessories*. "
        "Deterministic hash assignment ensures stable group membership. "
        "**This is a simulated retrospective experiment for analytical demonstration.**"
    )

    report_path = PROC_DIR / "ab_test_report.csv"
    stats_path  = PROC_DIR / "ab_test_stats.csv"

    if not report_path.exists() or not stats_path.exists():
        st.info(
            "A/B test outputs not found.\n\n"
            "Run `python3 src/06_ab_testing.py` (or `python3 run_all.py`) to generate results."
        )
        return

    metrics_df = pd.read_csv(report_path)
    stats_df   = pd.read_csv(stats_path)
    s = stats_df.iloc[0]

    # ── Summary significance badges ──────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        sig_conv = bool(s["conversion_significant"])
        color    = "green" if sig_conv else "gray"
        label    = "SIGNIFICANT" if sig_conv else "Not Significant"
        st.metric("Conversion Rate Test (χ²)", label, delta=f"p = {s['chi2_p_value']:.4f}")
    with col2:
        sig_aov = bool(s["aov_significant"])
        color   = "green" if sig_aov else "gray"
        label   = "SIGNIFICANT" if sig_aov else "Not Significant"
        st.metric("AOV Test (Welch's t)", label, delta=f"p = {s['t_p_value']:.4f}")

    st.divider()

    # ── Metric comparison table ───────────────────────────────────────────────
    st.subheader("Per-Arm Metrics")
    st.dataframe(
        metrics_df.style.format({
            "conversion_rate":  "{:.2%}",
            "aov":              "${:,.2f}",
            "gmv_total":        "${:,.0f}",
            "revenue_per_user": "${:,.2f}",
        }, na_rep="—"),
        use_container_width=True,
    )

    # ── AOV bar chart ─────────────────────────────────────────────────────────
    st.subheader("AOV: Control vs Treatment")
    if "arm" in metrics_df.columns and "aov" in metrics_df.columns:
        aov_data = metrics_df.set_index("arm")["aov"]
        st.bar_chart(aov_data, use_container_width=True)

    # ── Conversion rate lift ──────────────────────────────────────────────────
    st.subheader("Conversion Rate Lift")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.metric("Control Conversion",   f"{s['ctrl_conversion']:.2%}")
    with col_b:
        st.metric("Treatment Conversion", f"{s['treat_conversion']:.2%}")
    with col_c:
        st.metric(
            "Lift",
            f"{s['conversion_lift']:+.2%}",
            delta=f"95% CI [{s['ci_lower_95']:+.4f}, {s['ci_upper_95']:+.4f}]",
        )

    # ── Effect sizes ──────────────────────────────────────────────────────────
    st.subheader("Effect Size Summary")
    effect_df = pd.DataFrame({
        "Metric":       ["Conversion Rate", "AOV"],
        "Test":         ["Chi-squared", "Welch's t-test"],
        "Statistic":    [s["chi2_stat"], s["t_stat"]],
        "p-value":      [s["chi2_p_value"], s["t_p_value"]],
        "Cohen's d":    ["—", s["cohens_d"]],
        "Effect Size":  ["—", s["effect_size_label"]],
        "Significant":  [
            "Yes ✓" if s["conversion_significant"] else "No",
            "Yes ✓" if s["aov_significant"] else "No",
        ],
    })
    st.dataframe(effect_df, use_container_width=True, hide_index=True)

    st.caption(
        "_Cohen's d interpretation: negligible < 0.2, small 0.2–0.5, medium 0.5–0.8, large ≥ 0.8_"
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="Growth Analytics Platform",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    con = get_connection()

    st.sidebar.title("📊 Growth Analytics Platform")
    st.sidebar.caption("Ecommerce Product Intelligence")
    st.sidebar.divider()

    page = st.sidebar.radio(
        "Navigation",
        [
            "Executive KPIs",
            "Cohort Retention",
            "Churn Risk Explorer",
            "A/B Test Results",
        ],
        label_visibility="collapsed",
    )

    st.sidebar.divider()
    st.sidebar.caption("Data: Olist Brazilian E-Commerce (Kaggle)")
    st.sidebar.caption("Stack: DuckDB · Python · scikit-learn · Streamlit")

    dispatch = {
        "Executive KPIs":      page_executive_kpis,
        "Cohort Retention":    page_cohort_retention,
        "Churn Risk Explorer": page_churn_explorer,
        "A/B Test Results":    page_ab_results,
    }
    dispatch[page](con)


if __name__ == "__main__":
    main()
