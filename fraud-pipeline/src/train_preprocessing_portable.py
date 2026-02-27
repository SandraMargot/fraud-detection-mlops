import argparse
import os
import joblib
import numpy as np
import pandas as pd
import boto3

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


# -----------------------------
# Minimal feature engineering (kept simple for MLOps focus)
# -----------------------------
def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Common fraud dataset columns (if present)
    if "trans_date_trans_time" in df.columns:
        dt = pd.to_datetime(df["trans_date_trans_time"], errors="coerce")
        df["trans_hour"] = dt.dt.hour
        df["trans_dow"] = dt.dt.dayofweek
        df.drop(columns=["trans_date_trans_time"], inplace=True)

    if "dob" in df.columns:
        dob = pd.to_datetime(df["dob"], errors="coerce")
        # Age in years (approx)
        ref = pd.Timestamp("today").normalize()
        df["age"] = ((ref - dob).dt.days / 365.25).astype("float")
        df.drop(columns=["dob"], inplace=True)

    # Drop leakage / identifiers if present
    for col in ["trans_num", "unix_time", "first", "last", "street"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    return df


def build_preprocess(df_sample: pd.DataFrame) -> ColumnTransformer:
    # We decide column groups based on available columns
    numeric_candidates = [
        "amt", "zip", "lat", "long", "city_pop", "merch_lat", "merch_long",
        "trans_hour", "trans_dow", "age"
    ]
    hashed_text_candidates = ["merchant", "job"]  # high-cardinality text-like columns
    categorical_candidates = ["category", "gender", "city", "state"]

    numeric_cols = [c for c in numeric_candidates if c in df_sample.columns]
    hashed_cols = [c for c in hashed_text_candidates if c in df_sample.columns]
    cat_cols = [c for c in categorical_candidates if c in df_sample.columns]

    numeric_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])

    cat_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse=True)),
    ])

    # Hashing: impute then hash (FeatureHasher works on strings)
    hash_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("hash", HashingEncoder(n_features=128)),
    ])

    transformers = []
    if numeric_cols:
        transformers.append(("num", numeric_pipe, numeric_cols))
    if cat_cols:
        transformers.append(("cat", cat_pipe, cat_cols))
    if hashed_cols:
        # ColumnTransformer will pass a 2D array for these cols; HashingEncoder handles it
        transformers.append(("hash", hash_pipe, hashed_cols))

    if not transformers:
        raise ValueError("No known columns found to preprocess. Check input CSV columns.")

    return ColumnTransformer(transformers=transformers, remainder="drop", sparse_threshold=0.3)


def main():
    S3_BUCKET = "automated-fraud-detection"
    S3_KEY = "training/fraudTest.csv"

    OUTPUT_JOBLIB = os.path.join("artifacts", "preprocess.joblib")

    # --- Download from S3 ---
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj["Body"])

    # Drop target if present
    if "is_fraud" in df.columns:
        df = df.drop(columns=["is_fraud"])

    df = add_time_features(df)

    preprocess = build_preprocess(df)
    preprocess.fit(df)

    os.makedirs("artifacts", exist_ok=True)
    joblib.dump(preprocess, OUTPUT_JOBLIB)

    print("Saved:", OUTPUT_JOBLIB)


if __name__ == "__main__":
    main()
