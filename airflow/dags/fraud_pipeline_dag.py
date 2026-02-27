from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import time
import requests
import json
import pandas as pd
import joblib
import boto3
import os


# -----------------------
# Config
# -----------------------
API_URL = "https://sdacelo-real-time-fraud-detection.hf.space/current-transactions"
PREPROCESS_PATH = "/opt/airflow/artifacts/preprocess.joblib"

FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))

AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
SAGEMAKER_ENDPOINT_NAME = os.getenv("SAGEMAKER_ENDPOINT_NAME", "fraud-xgb-rt-eu-west-3")

# Email (via Airflow SMTP Connection)
SMTP_CONN_ID = os.getenv("SMTP_CONN_ID", "smtp_gmail")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")  # à définir dans ton .env / docker-compose


def log_step(msg: str):
    print(f"\n==================== {msg} ====================\n")


# -----------------------
# Tasks
# -----------------------
def extract_validate():
    log_step("START EXTRACT")
    time.sleep(2)

    r = requests.get(API_URL, headers={"accept": "application/json"}, timeout=20)
    r.raise_for_status()

    # L'API renvoie un JSON encodé dans une string JSON
    data = json.loads(json.loads(r.text))
    df = pd.DataFrame(data["data"], columns=data["columns"])

    if df.empty:
        raise ValueError("API returned empty dataframe")

    row = df.iloc[0].to_dict()
    trans_num = row.get("trans_num")
    if not trans_num:
        raise ValueError("Missing trans_num in payload")

    log_step(f"DONE EXTRACT - trans_num={trans_num}")
    return {"trans_num": trans_num, "row": row}


def transform(ti):
    log_step("START TRANSFORM")
    time.sleep(2)

    x = ti.xcom_pull(task_ids="extract_validate")
    row = x["row"]

    df = pd.DataFrame([row])

    X = df.drop(
        columns=[
            "is_fraud",
            "current_time",
            "trans_date_trans_time",
            "unix_time",
            "first",
            "last",
            "street",
            "city",
            "dob",
            "trans_num",
            "cc_num",
        ],
        errors="ignore",
    )

    if "gender" in X.columns:
        X["gender"] = X["gender"].map({"M": 0, "F": 1})

    preprocessor = joblib.load(PREPROCESS_PATH)
    Xt = preprocessor.transform(X)

    if hasattr(Xt, "toarray"):
        dense = Xt.toarray()[0]
    else:
        dense = Xt[0]

    log_step(f"DONE TRANSFORM - shape={getattr(Xt, 'shape', None)}")
    csv_line = ",".join(str(float(v)) for v in dense)

    return {"trans_num": x["trans_num"], "row": row, "csv_line": csv_line}


def score_sagemaker(ti):
    log_step("START SCORE (SAGEMAKER)")
    time.sleep(2)

    x = ti.xcom_pull(task_ids="transform")
    csv_line = x["csv_line"]

    smr = boto3.client("sagemaker-runtime", region_name=AWS_REGION)
    resp = smr.invoke_endpoint(
        EndpointName=SAGEMAKER_ENDPOINT_NAME,
        ContentType="text/csv",
        Body=csv_line.encode("utf-8"),
    )

    body = resp["Body"].read().decode("utf-8").strip()
    fraud_proba = float(body)
    fraud_flag = fraud_proba >= FRAUD_THRESHOLD

    log_step(f"DONE SCORE (SAGEMAKER) - proba={fraud_proba:.4f} flag={fraud_flag}")
    return {
        "trans_num": x["trans_num"],
        "row": x["row"],
        "fraud_proba": fraud_proba,
        "fraud_flag": fraud_flag,
        "model_version": "xgb-rt",
    }


def load_to_postgres(ti):
    log_step("START LOAD (POSTGRES UPSERT)")
    time.sleep(2)

    x = ti.xcom_pull(task_ids="score_sagemaker")
    row = x["row"]
    event_time = row.get("current_time")
    event_time = int(event_time)

    params = {
        "trans_num": x["trans_num"],
        "event_time": event_time,
        "cc_num": str(row.get("cc_num")) if row.get("cc_num") is not None else None,
        "merchant": row.get("merchant"),
        "category": row.get("category"),
        "amt": row.get("amt"),
        "first": row.get("first"),
        "last": row.get("last"),
        "gender": row.get("gender"),
        "street": row.get("street"),
        "city": row.get("city"),
        "state": row.get("state"),
        "zip": row.get("zip"),
        "lat": row.get("lat"),
        "long": row.get("long"),
        "city_pop": row.get("city_pop"),
        "job": row.get("job"),
        "dob": row.get("dob"),
        "merch_lat": row.get("merch_lat"),
        "merch_long": row.get("merch_long"),
        "fraud_proba": float(x["fraud_proba"]),
        "fraud_flag": bool(x["fraud_flag"]),
        "model_version": x["model_version"],
    }

    hook = PostgresHook(postgres_conn_id="payments_pg")

    sql = """
    INSERT INTO public.payments_scored (
        trans_num, event_time, cc_num, merchant, category, amt, first, last, gender,
        street, city, state, zip, lat, long, city_pop, job, dob, merch_lat, merch_long,
        fraud_proba, fraud_flag, model_version
    )
    VALUES (
        %(trans_num)s, %(event_time)s, %(cc_num)s, %(merchant)s, %(category)s, %(amt)s, %(first)s, %(last)s, %(gender)s,
        %(street)s, %(city)s, %(state)s, %(zip)s, %(lat)s, %(long)s, %(city_pop)s, %(job)s, %(dob)s, %(merch_lat)s, %(merch_long)s,
        %(fraud_proba)s, %(fraud_flag)s, %(model_version)s
    )
    ON CONFLICT (trans_num)
    DO UPDATE SET
        event_time    = EXCLUDED.event_time,
        cc_num        = EXCLUDED.cc_num,
        merchant      = EXCLUDED.merchant,
        category      = EXCLUDED.category,
        amt           = EXCLUDED.amt,
        first         = EXCLUDED.first,
        last          = EXCLUDED.last,
        gender        = EXCLUDED.gender,
        street        = EXCLUDED.street,
        city          = EXCLUDED.city,
        state         = EXCLUDED.state,
        zip           = EXCLUDED.zip,
        lat           = EXCLUDED.lat,
        long          = EXCLUDED.long,
        city_pop      = EXCLUDED.city_pop,
        job           = EXCLUDED.job,
        dob           = EXCLUDED.dob,
        merch_lat     = EXCLUDED.merch_lat,
        merch_long    = EXCLUDED.merch_long,
        fraud_proba   = EXCLUDED.fraud_proba,
        fraud_flag    = EXCLUDED.fraud_flag,
        model_version = EXCLUDED.model_version,
        ingested_at   = now();
    """

    hook.run(sql, parameters=params)

    log_step(f"DONE LOAD - upsert trans_num={params['trans_num']}")
    return {
        "trans_num": params["trans_num"],
        "fraud_flag": params["fraud_flag"],
        "fraud_proba": params["fraud_proba"],
    }


def is_fraud(ti) -> bool:
    """
    Gate task: si False => l'EmailOperator downstream sera SKIPPED
    """
    x = ti.xcom_pull(task_ids="load_to_postgres")
    return bool(x["fraud_flag"])


# -----------------------
# DAG
# -----------------------
default_args = {"retries": 0}

with DAG(
    dag_id="fraud_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["fraud", "mlops"],
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    t1 = PythonOperator(task_id="extract_validate", python_callable=extract_validate)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="score_sagemaker", python_callable=score_sagemaker)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t5 = ShortCircuitOperator(task_id="is_fraud", python_callable=is_fraud)

    t6 = EmailOperator(
        task_id="send_fraud_email",
        to=ALERT_EMAIL_TO,
        subject="Alerte paiement frauduleux - trans_num={{ ti.xcom_pull(task_ids='load_to_postgres')['trans_num'] }}",
        html_content="""
        <p>Bonjour,</p>
        <p><b>Transaction suspecte détectée</b></p>
        <ul>
          <li>trans_num: {{ ti.xcom_pull(task_ids='load_to_postgres')['trans_num'] }}</li>
          <li>fraud_proba: {{ '%.4f' | format(ti.xcom_pull(task_ids='load_to_postgres')['fraud_proba']) }}</li>
          <li>threshold: {{ params.threshold }}</li>
          <li>model_version: xgb-rt</li>
        </ul>
        """,
        conn_id=SMTP_CONN_ID,
        params={"threshold": FRAUD_THRESHOLD},
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
