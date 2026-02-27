import sagemaker
from sagemaker.image_uris import retrieve
from sagemaker.estimator import Estimator
from sagemaker.inputs import TrainingInput
import boto3

REGION = "eu-west-3"
ROLE_ARN = "arn:aws:iam::891084863160:role/service-role/SageMaker-FraudProject"

BUCKET = "automated-fraud-detection"
TRAIN_S3 = f"s3://{BUCKET}/training/processed/train.csv"
VAL_S3 = f"s3://{BUCKET}/training/processed/val.csv"

OUTPUT_S3 = f"s3://{BUCKET}/training/model-output/"

def compute_scale_pos_weight(s3_uri: str) -> float:
    """
    Compute scale_pos_weight = (#negative / #positive) from train.csv on S3.
    train.csv format: label is first column, no header.
    """
    s3 = boto3.client("s3", region_name=REGION)
    # parse s3://bucket/key
    assert s3_uri.startswith("s3://")
    _, _, rest = s3_uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)

    pos = 0
    neg = 0
    for line in obj["Body"].iter_lines():
        if not line:
            continue
        # first value before comma is label
        first = line.split(b",", 1)[0]
        if first == b"1" or first == b"1.0":
            pos += 1
        else:
            neg += 1

    if pos == 0:
        return 1.0
    return float(neg) / float(pos)


if __name__ == "__main__":
    sess = sagemaker.session.Session(boto_session=boto3.Session(region_name=REGION))

    xgb_image = retrieve(
        framework="xgboost",
        region=REGION,
        version="1.7-1",  # built-in XGBoost container
        py_version="py3",
        instance_type="ml.m5.4xlarge",
    )

    spw = compute_scale_pos_weight(TRAIN_S3)
    print(f"Computed scale_pos_weight: {spw:.4f}")

    xgb = Estimator(
        image_uri=xgb_image,
        role=ROLE_ARN,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        volume_size=30,
        max_run=3600,
        output_path=OUTPUT_S3,
        sagemaker_session=sess,
    )

    # Minimal hyperparams (pipeline-first)
    xgb.set_hyperparameters(
        objective="binary:logistic",
        num_round=80,
        max_depth=5,
        eta=0.2,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="auc",
        scale_pos_weight=spw,
    )

    inputs = {
        "train": TrainingInput(TRAIN_S3, content_type="text/csv"),
        "validation": TrainingInput(VAL_S3, content_type="text/csv"),
    }

    print("Starting training job...")
    xgb.fit(inputs=inputs)

    # Training job name is your model_version
    print(f"TrainingJobName (model_version): {xgb.latest_training_job.name}")
