# =========================================================
# ML TRAINING FROM DATA WAREHOUSE
# TASK: HIGH PROFIT vs LOW PROFIT (CLASSIFICATION)
# MODEL: GRADIENT BOOSTING CLASSIFIER
# =========================================================

import pandas as pd
import numpy as np
import pickle
import json
import os

from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report
)

# =========================================================
# CONFIG
# =========================================================
OUTPUT_PATH = "output"
MODEL_PATH = os.path.join(OUTPUT_PATH, "models")
os.makedirs(MODEL_PATH, exist_ok=True)

# =========================================================
# DATABASE CONFIG (DATA WAREHOUSE)
# =========================================================
DB_HOST = "localhost"        # GANTI ke "pustaka_postgres_dwh" jika run di Docker
DB_PORT = "5432"
DB_NAME = "pustaka_dwh"
DB_USER = "pustaka_admin"
DB_PASS = "pustaka2025"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# =========================================================
# LOAD DATA FROM DATA WAREHOUSE
# =========================================================
QUERY = """
SELECT
    fs.profit,
    fs.quantity,
    fs.total_amount,
    dc.category_oltp   AS category,
    dl.ship_city       AS city,
    dl.ship_state      AS state,
    dd.month,
    dd.quarter
FROM factsales fs
JOIN dimcategory dc ON fs.category_id = dc.category_id
JOIN dimlocation dl ON fs.city_id = dl.city_id
JOIN dimdate dd ON fs.date_id = dd.date_id
"""

# =========================================================
# TRAINING PIPELINE
# =========================================================
def train_gradient_boosting_classifier():

    print("\n" + "="*70)
    print(" 🚀 FINAL ML TRAINING — GRADIENT BOOSTING CLASSIFIER")
    print(" Task: HIGH PROFIT vs LOW PROFIT (FROM DATA WAREHOUSE)")
    print("="*70)

    # 1. Load data
    df = pd.read_sql(QUERY, engine)
    print(f"✓ Data loaded from DW: {df.shape}")

    # =====================================================
    # 2. DATA QUALITY CHECK
    # =====================================================
    print("\n🔍 DATA QUALITY CHECK")
    print(df.isnull().sum())

    # =====================================================
    # 3. BUILD LABEL (CLASSIFICATION)
    # =====================================================
    threshold = df["profit"].median()
    df["is_high_profit"] = (df["profit"] >= threshold).astype(int)

    print("\n🎯 LABEL DISTRIBUTION:")
    print(df["is_high_profit"].value_counts(normalize=True))

    # =====================================================
    # 4. ENCODING CATEGORICAL
    # =====================================================
    le_cat = LabelEncoder()
    le_city = LabelEncoder()
    le_state = LabelEncoder()

    df["category_encoded"] = le_cat.fit_transform(df["category"].astype(str))
    df["city_encoded"] = le_city.fit_transform(df["city"].astype(str))
    df["state_encoded"] = le_state.fit_transform(df["state"].astype(str))

    # Save encoders (BUKTI UAS)
    pickle.dump(le_cat, open(os.path.join(MODEL_PATH, "encoder_category.pkl"), "wb"))
    pickle.dump(le_city, open(os.path.join(MODEL_PATH, "encoder_city.pkl"), "wb"))
    pickle.dump(le_state, open(os.path.join(MODEL_PATH, "encoder_state.pkl"), "wb"))

    # =====================================================
    # 5. FEATURE ENGINEERING (NON-LINEAR)
    # =====================================================
    df["quantity_log"] = np.log1p(df["quantity"])
    df["is_bulk_order"] = (df["quantity"] >= 3).astype(int)

    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    FEATURES = [
        "category_encoded",
        "city_encoded",
        "state_encoded",
        "quarter",
        "month_sin",
        "month_cos",
        "quantity",
        "quantity_log",
        "is_bulk_order"
    ]

    TARGET = "is_high_profit"

    X = df[FEATURES]
    y = df[TARGET]

    # =====================================================
    # 6. TRAIN TEST SPLIT
    # =====================================================
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    # =====================================================
    # 7. SCALING
    # =====================================================
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    pickle.dump(scaler, open(os.path.join(MODEL_PATH, "feature_scaler.pkl"), "wb"))

    # =====================================================
    # 8. MODEL — GRADIENT BOOSTING CLASSIFIER
    # =====================================================
    model = GradientBoostingClassifier(
        n_estimators=150,
        learning_rate=0.05,
        max_depth=5,
        min_samples_split=10,
        min_samples_leaf=5,
        subsample=0.8,
        random_state=42
    )

    model.fit(X_train, y_train)

    # =====================================================
    # 9. EVALUATION
    # =====================================================
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred)
    rec = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc = roc_auc_score(y_test, y_proba)
    cm = confusion_matrix(y_test, y_pred)

    print("\n📊 MODEL PERFORMANCE")
    print(f"Accuracy : {acc:.4f}")
    print(f"Precision: {prec:.4f}")
    print(f"Recall   : {rec:.4f}")
    print(f"F1-score : {f1:.4f}")
    print(f"ROC-AUC  : {roc:.4f}")

    print("\n🧮 Confusion Matrix")
    print(cm)

    print("\n📑 Classification Report")
    print(classification_report(y_test, y_pred))

    # =====================================================
    # 10. SAVE MODEL
    # =====================================================
    model_path = os.path.join(MODEL_PATH, "gradient_boosting_classifier_final.pkl")
    pickle.dump(model, open(model_path, "wb"))

    print(f"\n✅ MODEL DISIMPAN: {model_path}")

    # =====================================================
    # 11. SAVE SUMMARY (UNTUK LAPORAN)
    # =====================================================
    summary = {
        "model": "Gradient Boosting Classifier",
        "task": "High Profit vs Low Profit",
        "features": FEATURES,
        "threshold_profit": float(threshold),
        "metrics": {
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1,
            "roc_auc": roc
        }
    }

    with open(os.path.join(OUTPUT_PATH, "ml_summary_classifier.json"), "w") as f:
        json.dump(summary, f, indent=4)

    print("\n🎯 TRAINING SELESAI — SIAP UNTUK DASHBOARD & UAS")
    print("="*70)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    train_gradient_boosting_classifier()
