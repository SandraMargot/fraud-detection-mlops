import pandas as pd
import numpy as np
import joblib
import boto3
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import FeatureHasher
from sklearn.base import BaseEstimator, TransformerMixin
from encoders import HashingEncoder

# -----------------------------
# CONFIG
# -----------------------------
S3_BUCKET = "automated-fraud-detection"
S3_KEY = "training/fraudTest.csv"

TRAIN_OUTPUT = "artifacts/train.csv"
VAL_OUTPUT = "artifacts/val.csv"
PREPROCESS_OUTPUT = "artifacts/preprocess.joblib"


# -----------------------------
# Load data from S3
# -----------------------------
def load_data():
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj["Body"])
    return df


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":

    print("Loading data from S3...")
    df = load_data()

    # Drop useless index column (first empty column)
    df = df.iloc[:, 1:]

    # Drop excluded columns
    df = df.drop(columns=[
        "trans_date_trans_time",
        "unix_time",
        "first",
        "last",
        "street",
        "city",
        "dob",
        "trans_num"
    ])

    # Target
    y = df["is_fraud"]
    X = df.drop(columns=["is_fraud", "cc_num"])  # drop cc_num

    # Basic preprocessing
    X["gender"] = X["gender"].map({"M": 0, "F": 1})

    categorical_low = ["category", "state"]
    categorical_high = ["merchant", "job"]
    numeric_features = [
        "amt", "zip", "lat", "long",
        "city_pop", "merch_lat", "merch_long"
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("low_cat", OneHotEncoder(handle_unknown="ignore"), categorical_low),
            ("high_cat", HashingEncoder(n_features=128), categorical_high),
            ("num", "passthrough", numeric_features)
        ]
    )

    print("Applying preprocessing...")
    X_processed = preprocessor.fit_transform(X)

    # Train/Validation split
    X_train, X_val, y_train, y_val = train_test_split(
        X_processed,
        y,
        test_size=0.2,
        stratify=y,
        random_state=42
    )

    # Convert to numpy and stack label first
    train_array = np.column_stack((y_train.values, X_train.toarray() if hasattr(X_train, "toarray") else X_train))
    val_array = np.column_stack((y_val.values, X_val.toarray() if hasattr(X_val, "toarray") else X_val))

    print("Saving train/val CSV...")
    np.savetxt(TRAIN_OUTPUT, train_array, delimiter=",", fmt="%s")
    np.savetxt(VAL_OUTPUT, val_array, delimiter=",", fmt="%s")

    print("Saving preprocessing artifact...")
    joblib.dump(preprocessor, PREPROCESS_OUTPUT)

    print("Done.")
