"""
06_ab_testing.py
────────────────
A/B testing framework for a simulated pricing experiment.

Experiment design:
  - Target categories: 'electronics', 'computers_accessories'
  - Control   : standard item prices
  - Treatment : item prices × 1.10 (+10% increase)
  - Assignment: deterministic hash of customer_unique_id (stable across runs)

Metrics measured:
  - Conversion rate (orders / eligible users)
  - AOV             (average order value per converting user)
  - GMV             (total gross merchandise value)
  - Revenue per user

Statistical tests:
  - chi-squared      → conversion rate difference
  - two-sample t-test → AOV difference (Welch's t-test)
  - Cohen's d        → AOV effect size
  - 95% CI           → conversion rate lift

NOTE: This is a retrospective simulation applied to historical data to
demonstrate the analytical framework. Results should be interpreted as
illustrative, not as evidence of a real pricing experiment.

Usage:
    python3 src/06_ab_testing.py --db_path outputs/warehouse.duckdb --out_dir data/processed
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import stats


TARGET_CATEGORIES  = ("electronics", "computers_accessories")
PRICE_MULTIPLIER   = 1.10
ASSIGNMENT_SALT    = "pricing_v1"
ALPHA              = 0.05


def assign_treatment(customer_unique_id: str, salt: str = ASSIGNMENT_SALT) -> str:
    """
    Deterministic group assignment via MD5 hash.
    Stable across runs — same customer always lands in the same arm.
    """
    digest = hashlib.md5(f"{salt}:{customer_unique_id}".encode()).digest()
    return "treatment" if digest[0] % 2 == 1 else "control"


def build_experiment_dataset(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Experiment pool = ALL unique customers.
    Each customer is assigned deterministically to control or treatment.
    Conversion = placed at least one order in the target categories.

    This design correctly produces a non-trivial non-converting population
    (most customers never bought electronics), enabling a valid conversion
    rate comparison and AOV comparison among converters.
    """
    # Full customer pool
    all_customers = con.execute(
        "SELECT DISTINCT customer_unique_id FROM dim.dim_customers;"
    ).df()

    if all_customers.empty:
        raise SystemExit("No customers found. Check the warehouse.")

    all_customers["arm"] = all_customers["customer_unique_id"].map(assign_treatment)

    # Orders in target categories (conversion events)
    orders_df = con.execute("""
        SELECT
            c.customer_unique_id,
            fi.order_id,
            fi.item_price,
            fi.freight_value
        FROM fct.fct_order_items fi
        JOIN dim.dim_customers c ON fi.customer_id = c.customer_id
        WHERE fi.product_category_name_en IN ('electronics', 'computers_accessories')
          AND fi.order_status IN ('delivered','shipped','invoiced','processing','approved')
    """).df()

    # Left join: non-converting customers get NaN for order columns
    df = all_customers.merge(orders_df, on="customer_unique_id", how="left")
    df["converted"] = df["order_id"].notna().astype(int)

    # Apply +10% price to treatment converters only
    df["item_price_adj"] = np.where(
        (df["arm"] == "treatment") & (df["converted"] == 1),
        df["item_price"] * PRICE_MULTIPLIER,
        df["item_price"],
    )
    df["order_gmv_adj"] = np.where(
        df["converted"] == 1,
        df["item_price_adj"] + df["freight_value"],
        np.nan,
    )

    return df


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-arm metrics.
    n_users         = all assigned users (converting + non-converting)
    n_converted     = users with at least one order in target categories
    conversion_rate = n_converted / n_users
    aov             = mean order GMV among converting users only
    """
    # Total users per arm
    pool = df.drop_duplicates("customer_unique_id")[["customer_unique_id", "arm"]]
    arm_sizes = pool.groupby("arm").size().rename("n_users")

    # Converters only
    converters = df[df["converted"] == 1]

    # Unique converting users per arm
    n_converted = (
        converters.groupby("arm")["customer_unique_id"].nunique().rename("n_converted")
    )

    # Order-level GMV aggregated per (arm, order)
    order_gmv = (
        converters.groupby(["arm", "order_id"])["order_gmv_adj"]
        .sum()
        .reset_index()
    )
    order_stats = order_gmv.groupby("arm").agg(
        n_orders  =("order_id",      "count"),
        gmv_total =("order_gmv_adj", "sum"),
        aov       =("order_gmv_adj", "mean"),
    )

    metrics = pd.concat([arm_sizes, n_converted, order_stats], axis=1).reset_index()
    metrics["conversion_rate"]  = metrics["n_converted"] / metrics["n_users"]
    metrics["revenue_per_user"] = metrics["gmv_total"] / metrics["n_users"]

    return metrics[[
        "arm", "n_users", "n_converted", "conversion_rate",
        "aov", "gmv_total", "revenue_per_user",
    ]]


def run_statistical_tests(df: pd.DataFrame) -> dict:
    """
    Statistical tests comparing control vs treatment.

    Chi-squared: user-level conversion (converted / not-converted).
    Welch's t-test + Cohen's d: order-level AOV among converters.
    95% CI: normal approximation for conversion rate lift.
    """
    pool = df.drop_duplicates("customer_unique_id")[["customer_unique_id", "arm", "converted"]]
    ctrl_pool  = pool[pool["arm"] == "control"]
    treat_pool = pool[pool["arm"] == "treatment"]

    ctrl_users  = len(ctrl_pool)
    treat_users = len(treat_pool)
    ctrl_conv   = ctrl_pool["converted"].sum()
    treat_conv  = treat_pool["converted"].sum()

    # ── Chi-squared: conversion contingency table ────────────────────────────
    # [[converted_ctrl, not_converted_ctrl], [converted_treat, not_converted_treat]]
    contingency = np.array([
        [ctrl_conv,  ctrl_users  - ctrl_conv],
        [treat_conv, treat_users - treat_conv],
    ], dtype=float)
    chi2_stat, chi2_p, _, _ = stats.chi2_contingency(contingency)

    # ── Welch's t-test: AOV among converters ────────────────────────────────
    converters = df[df["converted"] == 1]
    order_gmv = converters.groupby(["arm", "order_id"])["order_gmv_adj"].sum().reset_index()
    ctrl_aov_vals  = order_gmv[order_gmv["arm"] == "control"]["order_gmv_adj"].values
    treat_aov_vals = order_gmv[order_gmv["arm"] == "treatment"]["order_gmv_adj"].values

    if len(ctrl_aov_vals) < 2 or len(treat_aov_vals) < 2:
        t_stat, t_p = np.nan, np.nan
        cohens_d = np.nan
    else:
        t_stat, t_p = stats.ttest_ind(treat_aov_vals, ctrl_aov_vals, equal_var=False)
        pooled_std = np.sqrt(
            (np.std(treat_aov_vals, ddof=1)**2 + np.std(ctrl_aov_vals, ddof=1)**2) / 2
        )
        cohens_d = (np.mean(treat_aov_vals) - np.mean(ctrl_aov_vals)) / max(pooled_std, 1e-9)

    # ── 95% CI for conversion rate lift (normal approximation) ───────────────
    p1 = ctrl_conv  / ctrl_users
    p2 = treat_conv / treat_users
    se = np.sqrt(p1 * (1 - p1) / ctrl_users + p2 * (1 - p2) / treat_users)
    lift_diff = p2 - p1
    ci_lower  = lift_diff - 1.96 * se
    ci_upper  = lift_diff + 1.96 * se

    aov_sig = (not np.isnan(t_p)) and (t_p < ALPHA)
    return {
        "ctrl_users":         ctrl_users,
        "treat_users":        treat_users,
        "ctrl_converted":     int(ctrl_conv),
        "treat_converted":    int(treat_conv),
        "ctrl_aov":           round(float(np.mean(ctrl_aov_vals)),  2) if len(ctrl_aov_vals)  else 0.0,
        "treat_aov":          round(float(np.mean(treat_aov_vals)), 2) if len(treat_aov_vals) else 0.0,
        "ctrl_conversion":    round(p1, 4),
        "treat_conversion":   round(p2, 4),
        "conversion_lift":    round(lift_diff, 4),
        "ci_lower_95":        round(ci_lower, 4),
        "ci_upper_95":        round(ci_upper, 4),
        "chi2_stat":          round(float(chi2_stat), 4),
        "chi2_p_value":       round(float(chi2_p), 6),
        "conversion_significant": bool(chi2_p < ALPHA),
        "t_stat":             round(float(t_stat), 4) if not np.isnan(t_stat) else None,
        "t_p_value":          round(float(t_p), 6)   if not np.isnan(t_p)    else None,
        "aov_significant":    aov_sig,
        "cohens_d":           round(float(cohens_d), 4) if not np.isnan(cohens_d) else None,
        "effect_size_label":  _label_effect_size(abs(cohens_d)) if not np.isnan(cohens_d) else "n/a",
        "alpha":              ALPHA,
    }


def _label_effect_size(d: float) -> str:
    if d < 0.2:
        return "negligible"
    if d < 0.5:
        return "small"
    if d < 0.8:
        return "medium"
    return "large"


def format_report(metrics: pd.DataFrame, stats_dict: dict) -> str:
    """Build a human-readable experiment report string."""
    sig_conv = "SIGNIFICANT ✓" if stats_dict["conversion_significant"] else "not significant"
    sig_aov  = "SIGNIFICANT ✓" if stats_dict["aov_significant"]        else "not significant"

    lines = [
        "",
        "=" * 60,
        "  A/B TEST REPORT — Pricing Experiment",
        "  Target categories: electronics, computers_accessories",
        f"  Treatment: +{(PRICE_MULTIPLIER - 1):.0%} price increase",
        f"  Significance level: α = {stats_dict['alpha']}",
        "=" * 60,
        "",
        "  PER-ARM METRICS",
        "  " + metrics.to_string(index=False),
        "",
        "  STATISTICAL TESTS",
        f"  Conversion rate (chi-squared)",
        f"    Control   : {stats_dict['ctrl_conversion']:.2%}",
        f"    Treatment : {stats_dict['treat_conversion']:.2%}",
        f"    Lift      : {stats_dict['conversion_lift']:+.2%}  "
        f"(95% CI: [{stats_dict['ci_lower_95']:+.4f}, {stats_dict['ci_upper_95']:+.4f}])",
        f"    χ² = {stats_dict['chi2_stat']:.4f},  p = {stats_dict['chi2_p_value']:.6f}  →  {sig_conv}",
        "",
        f"  AOV (Welch's t-test)",
        f"    Control   : ${stats_dict['ctrl_aov']:.2f}",
        f"    Treatment : ${stats_dict['treat_aov']:.2f}",
        f"    Δ AOV     : ${stats_dict['treat_aov'] - stats_dict['ctrl_aov']:+.2f}",
        f"    t = {stats_dict['t_stat']:.4f},  p = {stats_dict['t_p_value']:.6f}  →  {sig_aov}",
        f"    Cohen's d : {stats_dict['cohens_d']:.4f}  ({stats_dict['effect_size_label']} effect)",
        "",
        "=" * 60,
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run A/B pricing experiment analysis.")
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb", help="DuckDB warehouse path")
    ap.add_argument("--out_dir", default="data/processed",           help="Directory for report outputs")
    args = ap.parse_args()

    np.random.seed(42)

    db_path = Path(args.db_path).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building experiment dataset...")
    con = duckdb.connect(str(db_path), read_only=True)
    exp_df = build_experiment_dataset(con)
    con.close()

    pool_counts = exp_df.drop_duplicates("customer_unique_id").groupby("arm").size()
    print(f"  Control   : {pool_counts.get('control', 0):,} users (pool)")
    print(f"  Treatment : {pool_counts.get('treatment', 0):,} users (pool)")
    conv_counts = exp_df[exp_df["converted"] == 1].groupby("arm")["customer_unique_id"].nunique()
    print(f"  Control converters   : {conv_counts.get('control', 0):,}")
    print(f"  Treatment converters : {conv_counts.get('treatment', 0):,}")

    print("\nComputing experiment metrics...")
    metrics_df = compute_metrics(exp_df)

    print("Running statistical tests...")
    stats_dict = run_statistical_tests(exp_df)

    report_str = format_report(metrics_df, stats_dict)
    print(report_str)

    # Save outputs
    report_path = out_dir / "ab_test_report.csv"
    metrics_df.to_csv(report_path, index=False)
    print(f"Saved: {report_path}")

    stats_path = out_dir / "ab_test_stats.csv"
    pd.DataFrame([stats_dict]).to_csv(stats_path, index=False)
    print(f"Saved: {stats_path}")

    # Also save the full text report
    txt_path = out_dir / "ab_test_report.txt"
    txt_path.write_text(report_str, encoding="utf-8")
    print(f"Saved: {txt_path}")

    print("\nOK: A/B testing analysis complete.")


if __name__ == "__main__":
    main()
