"""
05_churn_model.py
─────────────────
Train a churn prediction model on the engineered feature matrix.

Models trained:
  1. GradientBoostingClassifier — primary model (RandomizedSearchCV tuning)
  2. RandomForestClassifier     — comparison baseline

Evaluation:
  - ROC-AUC, Precision, Recall, F1 (test set)
  - Confusion matrix PNG
  - Feature importance chart PNG
  - Classification report printed to stdout

Outputs:
  - models/churn_model.pkl               — best pipeline (joblib)
  - outputs/confusion_matrix_gbm.png
  - outputs/feature_importance_gbm.png
  - outputs/metrics_comparison.csv
  - DuckDB: analytics.churn_predictions  — probability scores for all customers

Usage:
    python3 src/05_churn_model.py \\
        --db_path outputs/warehouse.duckdb \\
        --features_path data/processed/features.parquet \\
        --out_dir outputs \\
        --models_dir models
"""

from __future__ import annotations

import argparse
import warnings
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore", category=UserWarning)

LABEL_COL = "is_churned"
ID_COL    = "customer_unique_id"
RANDOM_STATE = 42


def load_features(features_path: Path) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """Load features.parquet and return (X, y, customer_ids)."""
    df = pd.read_parquet(features_path)
    y   = df[LABEL_COL]
    ids = df[ID_COL]
    X   = df.drop(columns=[LABEL_COL, ID_COL])
    return X, y, ids


def build_preprocessor(numeric_cols: list[str], passthrough_cols: list[str]) -> ColumnTransformer:
    """StandardScaler on numeric features; pass through OHE payment columns."""
    return ColumnTransformer(
        transformers=[
            ("scale", StandardScaler(), numeric_cols),
            ("pass",  "passthrough",    passthrough_cols),
        ],
        remainder="drop",
    )


def train_gradient_boosting(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    preprocessor: ColumnTransformer,
) -> Pipeline:
    """
    GradientBoostingClassifier wrapped in a sklearn Pipeline.
    Tuned via RandomizedSearchCV (n_iter=20, cv=5, scoring=roc_auc).
    """
    param_dist = {
        "clf__n_estimators":  [100, 200, 300],
        "clf__max_depth":     [3, 4, 5],
        "clf__learning_rate": [0.05, 0.10, 0.15],
        "clf__subsample":     [0.70, 0.85, 1.0],
        "clf__min_samples_leaf": [10, 20, 40],
    }
    base_pipeline = Pipeline([
        ("prep", preprocessor),
        ("clf",  GradientBoostingClassifier(random_state=RANDOM_STATE)),
    ])
    search = RandomizedSearchCV(
        base_pipeline,
        param_distributions=param_dist,
        n_iter=20,
        cv=5,
        scoring="roc_auc",
        n_jobs=-1,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    search.fit(X_train, y_train)
    print(f"\nGBM best params:   {search.best_params_}")
    print(f"GBM best CV AUC:   {search.best_score_:.4f}")
    return search.best_estimator_


def train_random_forest(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    preprocessor: ColumnTransformer,
) -> Pipeline:
    """
    RandomForestClassifier pipeline as a comparison baseline.
    """
    from sklearn.model_selection import GridSearchCV

    param_grid = {
        "clf__n_estimators": [100, 200],
        "clf__max_depth":    [None, 10, 20],
    }
    base_pipeline = Pipeline([
        ("prep", preprocessor),
        ("clf",  RandomForestClassifier(
            class_weight="balanced",
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )),
    ])
    search = GridSearchCV(
        base_pipeline,
        param_grid=param_grid,
        cv=3,
        scoring="roc_auc",
        n_jobs=-1,
        verbose=0,
    )
    search.fit(X_train, y_train)
    print(f"\nRF  best params:   {search.best_params_}")
    print(f"RF  best CV AUC:   {search.best_score_:.4f}")
    return search.best_estimator_


def evaluate_model(
    pipeline: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    model_name: str,
    out_dir: Path,
) -> dict[str, float]:
    """
    Evaluate pipeline on test set. Saves confusion matrix PNG and feature
    importance chart PNG. Returns metric dict.
    """
    y_pred  = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    auc       = roc_auc_score(y_test, y_proba)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall    = recall_score(y_test, y_pred, zero_division=0)
    f1        = f1_score(y_test, y_pred, zero_division=0)

    print(f"\n{'─'*50}")
    print(f"  {model_name} — Test Set Performance")
    print(f"{'─'*50}")
    print(f"  ROC-AUC  : {auc:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall   : {recall:.4f}")
    print(f"  F1       : {f1:.4f}")
    print()
    print(classification_report(y_test, y_pred, target_names=["Not Churned", "Churned"]))

    # Confusion matrix
    fig, ax = plt.subplots(figsize=(5, 4))
    ConfusionMatrixDisplay.from_predictions(
        y_test, y_pred,
        display_labels=["Not Churned", "Churned"],
        ax=ax, colorbar=False,
    )
    ax.set_title(f"{model_name} — Confusion Matrix\nROC-AUC: {auc:.4f}")
    plt.tight_layout()
    cm_path = out_dir / f"confusion_matrix_{model_name.lower().replace(' ', '_')}.png"
    fig.savefig(cm_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {cm_path}")

    # Feature importance (GBM and RF expose feature_importances_)
    clf = pipeline.named_steps.get("clf")
    if hasattr(clf, "feature_importances_"):
        prep    = pipeline.named_steps["prep"]
        feat_names = list(X_test.columns)  # original names before transform

        # sklearn ColumnTransformer preserves feature order: scale cols first, then pass cols
        importances = clf.feature_importances_
        n = min(len(importances), len(feat_names))
        imp_series = pd.Series(importances[:n], index=feat_names[:n]).sort_values(ascending=True)
        top20 = imp_series.tail(20)

        fig, ax = plt.subplots(figsize=(8, 6))
        top20.plot(kind="barh", ax=ax, color="#2563eb")
        ax.set_xlabel("Importance")
        ax.set_title(f"{model_name} — Top-20 Feature Importances")
        plt.tight_layout()
        fi_path = out_dir / f"feature_importance_{model_name.lower().replace(' ', '_')}.png"
        fig.savefig(fi_path, dpi=150)
        plt.close(fig)
        print(f"  Saved: {fi_path}")

    return {
        "model":     model_name,
        "roc_auc":   round(auc, 4),
        "precision": round(precision, 4),
        "recall":    round(recall, 4),
        "f1":        round(f1, 4),
    }


def write_predictions_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    pipeline: Pipeline,
    X_all: pd.DataFrame,
    ids_all: pd.Series,
    model_name: str,
) -> None:
    """Score all customers and write predictions to analytics.churn_predictions."""
    proba = pipeline.predict_proba(X_all)[:, 1]
    pred  = (proba >= 0.5).astype(int)
    now   = datetime.now(tz=timezone.utc)

    pred_df = pd.DataFrame({
        "customer_unique_id": ids_all.values,
        "churn_probability":  proba,
        "predicted_churned":  pred,
        "model_name":         model_name,
        "scored_at":          now,
    })

    con.execute("DELETE FROM analytics.churn_predictions;")
    con.register("_pred_df", pred_df)
    con.execute("INSERT INTO analytics.churn_predictions SELECT * FROM _pred_df;")
    con.unregister("_pred_df")

    cnt = con.execute("SELECT COUNT(*) FROM analytics.churn_predictions;").fetchone()[0]
    print(f"\n  Written {cnt:,} predictions to analytics.churn_predictions")


def main() -> None:
    ap = argparse.ArgumentParser(description="Train churn prediction model.")
    ap.add_argument("--db_path",       default="outputs/warehouse.duckdb",       help="DuckDB warehouse path")
    ap.add_argument("--features_path", default="data/processed/features.parquet", help="Feature parquet path")
    ap.add_argument("--out_dir",       default="outputs",                         help="Output directory for charts/metrics")
    ap.add_argument("--models_dir",    default="models",                          help="Directory to save model artifact")
    args = ap.parse_args()

    np.random.seed(RANDOM_STATE)

    db_path       = Path(args.db_path).resolve()
    features_path = Path(args.features_path).resolve()
    out_dir       = Path(args.out_dir).resolve()
    models_dir    = Path(args.models_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    # ── Load features ──────────────────────────────────────────────────────
    print(f"Loading features from {features_path}...")
    X, y, ids = load_features(features_path)
    print(f"  {len(X):,} rows × {X.shape[1]} features")
    print(f"  Churn rate: {y.mean():.1%}")

    # ── Train/test split ───────────────────────────────────────────────────
    X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
        X, y, ids, test_size=0.2, stratify=y, random_state=RANDOM_STATE
    )
    print(f"\nTrain: {len(X_train):,}  |  Test: {len(X_test):,}")

    # ── Preprocessor ──────────────────────────────────────────────────────
    numeric_cols     = [c for c in X.columns if not c.startswith("pay_")]
    passthrough_cols = [c for c in X.columns if c.startswith("pay_")]
    preprocessor     = build_preprocessor(numeric_cols, passthrough_cols)

    # ── Train GBM (primary) ────────────────────────────────────────────────
    print("\n" + "="*50)
    print("Training GradientBoostingClassifier...")
    print("="*50)
    gbm_pipeline = train_gradient_boosting(X_train, y_train, build_preprocessor(numeric_cols, passthrough_cols))

    # ── Train RF (comparison) ──────────────────────────────────────────────
    print("\n" + "="*50)
    print("Training RandomForestClassifier (baseline)...")
    print("="*50)
    rf_pipeline = train_random_forest(X_train, y_train, build_preprocessor(numeric_cols, passthrough_cols))

    # ── Evaluate both ──────────────────────────────────────────────────────
    gbm_metrics = evaluate_model(gbm_pipeline, X_test, y_test, "Gradient Boosting", out_dir)
    rf_metrics  = evaluate_model(rf_pipeline,  X_test, y_test, "Random Forest",     out_dir)

    # Save comparison table
    metrics_df = pd.DataFrame([gbm_metrics, rf_metrics])
    metrics_path = out_dir / "metrics_comparison.csv"
    metrics_df.to_csv(metrics_path, index=False)
    print(f"\nSaved: {metrics_path}")
    print("\nModel Comparison:")
    print(metrics_df.to_string(index=False))

    # ── Pick best model by ROC-AUC ─────────────────────────────────────────
    best_pipeline = gbm_pipeline if gbm_metrics["roc_auc"] >= rf_metrics["roc_auc"] else rf_pipeline
    best_name     = gbm_metrics["model"] if gbm_metrics["roc_auc"] >= rf_metrics["roc_auc"] else rf_metrics["model"]
    print(f"\nBest model: {best_name} (ROC-AUC: {max(gbm_metrics['roc_auc'], rf_metrics['roc_auc']):.4f})")

    # ── Save model artifact ────────────────────────────────────────────────
    model_path = models_dir / "churn_model.pkl"
    joblib.dump(best_pipeline, model_path)
    print(f"Saved: {model_path}")

    # ── Write predictions to DuckDB ────────────────────────────────────────
    print("\nWriting predictions to DuckDB...")
    con = duckdb.connect(str(db_path))
    write_predictions_to_duckdb(con, best_pipeline, X, ids, best_name)
    con.close()

    print("\nOK: churn model training complete.")


if __name__ == "__main__":
    main()
