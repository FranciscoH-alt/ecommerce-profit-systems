"""
04_feature_engineering.py
─────────────────────────
Reads analytics.churn_signals from DuckDB and produces an ML-ready feature
matrix written to data/processed/features.parquet.

Transformations applied:
  - log1p(monetary_total)  → log_monetary  (right-skew correction)
  - recency_days capped at 365              (outlier dampening)
  - inter_purchase_interval_avg: -1 sentinel preserved (single-order customers)
  - preferred_payment_type: one-hot encoded
  - All numeric columns validated for zero NaN before writing

Usage:
    python3 src/04_feature_engineering.py --db_path outputs/warehouse.duckdb --out_dir data/processed
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


NUMERIC_FEATURES = [
    "recency_days_capped",
    "frequency",
    "log_monetary",
    "monetary_avg",
    "r_score",
    "f_score",
    "m_score",
    "customer_lifespan_days",
    "inter_purchase_interval_avg",
    "purchase_velocity_per_30d",
    "unique_products",
    "unique_categories",
    "avg_review_score",
    "pct_low_reviews",
    "installment_avg",
    "weekday_purchase_pct",
    "weekend_purchase_pct",
]

LABEL_COL = "is_churned"
ID_COL    = "customer_unique_id"


def load_churn_signals(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Query analytics.churn_signals and return the raw feature frame."""
    return con.execute("SELECT * FROM analytics.churn_signals;").df()


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply feature transformations and one-hot encode categorical columns.
    Returns a clean feature DataFrame with NUMERIC_FEATURES + OHE columns + label.
    """
    out = df[[ID_COL, LABEL_COL]].copy()

    # Skew correction: log1p of monetary value
    out["log_monetary"] = np.log1p(df["monetary_total"])

    # Recency: cap at 365 days to dampen extreme outliers
    out["recency_days_capped"] = df["recency_days"].clip(upper=365)

    # Pass through numeric features
    passthrough = [
        "frequency", "monetary_avg", "r_score", "f_score", "m_score",
        "customer_lifespan_days", "inter_purchase_interval_avg",
        "purchase_velocity_per_30d", "unique_products", "unique_categories",
        "avg_review_score", "pct_low_reviews", "installment_avg",
        "weekday_purchase_pct", "weekend_purchase_pct",
    ]
    for col in passthrough:
        out[col] = df[col]

    # One-hot encode preferred_payment_type
    payment_dummies = pd.get_dummies(
        df["preferred_payment_type"].fillna("credit_card"),
        prefix="pay",
        dtype=int,
    )
    out = pd.concat([out, payment_dummies], axis=1)

    return out


def validate_features(df: pd.DataFrame) -> None:
    """
    Assert no unexpected NaNs remain in numeric feature columns.
    Logs class imbalance to help the modeller set class_weight.
    """
    numeric_cols = NUMERIC_FEATURES + [c for c in df.columns if c.startswith("pay_")]
    null_counts = df[numeric_cols].isnull().sum()
    bad = null_counts[null_counts > 0]
    if not bad.empty:
        raise ValueError(
            f"Feature validation failed — NaN values found:\n{bad.to_string()}\n"
            "Check analytics.churn_signals for missing COALESCE defaults."
        )

    churn_rate = df[LABEL_COL].mean()
    n_churned  = df[LABEL_COL].sum()
    n_total    = len(df)
    print(f"\nClass distribution:")
    print(f"  churned    : {n_churned:,} ({churn_rate:.1%})")
    print(f"  not churned: {n_total - n_churned:,} ({1 - churn_rate:.1%})")
    print(f"  imbalance ratio: {(n_total - n_churned) / max(n_churned, 1):.1f}:1")
    print("  → use class_weight='balanced' in ML models")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build ML feature matrix from analytics.churn_signals.")
    ap.add_argument("--db_path", default="outputs/warehouse.duckdb", help="Path to DuckDB warehouse file")
    ap.add_argument("--out_dir", default="data/processed",           help="Directory for feature outputs")
    args = ap.parse_args()

    db_path = Path(args.db_path).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading churn signals from DuckDB...")
    con = duckdb.connect(str(db_path), read_only=True)
    raw_df = load_churn_signals(con)
    con.close()
    print(f"  {len(raw_df):,} customer records loaded")

    print("\nEngineering features...")
    feature_df = engineer_features(raw_df)

    print("\nValidating feature matrix...")
    validate_features(feature_df)

    # Save feature matrix
    features_path = out_dir / "features.parquet"
    feature_df.to_parquet(features_path, index=False)
    print(f"\nSaved: {features_path}")

    # Save summary statistics for reference
    summary_path = out_dir / "feature_summary.csv"
    numeric_cols = NUMERIC_FEATURES + [c for c in feature_df.columns if c.startswith("pay_")]
    feature_df[numeric_cols].describe().T.to_csv(summary_path)
    print(f"Saved: {summary_path}")

    print(f"\nOK: feature matrix ready — {feature_df.shape[0]:,} rows × {feature_df.shape[1]} columns")


if __name__ == "__main__":
    main()
