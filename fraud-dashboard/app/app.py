import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Fraud Monitoring", page_icon="🛡️", layout="wide")
st.title("Fraud Monitoring Dashboard")
st.caption("Daily check (J-1) — PostgreSQL → Streamlit")


# -----------------------------
# DB connection (env vars)
# -----------------------------
def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    host = env_required("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = env_required("PGDATABASE")
    user = env_required("PGUSER")
    password = env_required("PGPASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


engine = get_engine()


# -----------------------------
# Sidebar
# -----------------------------
default_day = date.today()

with st.sidebar:
    st.header("Filters")
    selected_day = st.date_input("Report date (J-1 default)", value=default_day)
    fraud_only = st.checkbox("Frauds only", value=False)
    st.caption("note: if missing event_time will be replaced with selected date.")


# -----------------------------
# Data access
# -----------------------------
@st.cache_data(ttl=15, show_spinner=False)
def load_kpis_for_day(day: date) -> dict:
    """
    KPI calculés sur la journée d'ingestion (ingested_at) — robuste pour la démo.
    """
    q = text("""
        SELECT
            COUNT(*)::bigint AS total_payments,
            COUNT(*) FILTER (WHERE fraud_flag IS TRUE)::bigint AS total_frauds,
            COALESCE(
                (COUNT(*) FILTER (WHERE fraud_flag IS TRUE)::numeric / NULLIF(COUNT(*)::numeric, 0)),
                0
            ) AS fraud_rate,
            COALESCE(SUM(amt) FILTER (WHERE fraud_flag IS TRUE), 0) AS fraud_amount
        FROM public.payments_scored
        WHERE DATE(ingested_at) = :day
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"day": day}).mappings().one()

    return {
        "total_payments": int(row["total_payments"]),
        "total_frauds": int(row["total_frauds"]),
        "fraud_rate": float(row["fraud_rate"]),
        "fraud_amount": float(row["fraud_amount"]),
    }


@st.cache_data(ttl=15, show_spinner=False)
def load_transactions_for_day(day: date, fraud_only: bool) -> pd.DataFrame:
    q = text("""
        SELECT
            trans_num,
            to_timestamp(event_time) AS event_time,
            amt,
            fraud_flag,
            fraud_proba,
            merchant,
            category,
            state,
            ingested_at
        FROM public.payments_scored
        WHERE DATE(ingested_at) = :day
          AND (:fraud_only = FALSE OR fraud_flag IS TRUE)
        ORDER BY ingested_at DESC NULLS LAST
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"day": day, "fraud_only": fraud_only})
    return df


# -----------------------------
# Main
# -----------------------------
try:
    # KPI row
    kpis = load_kpis_for_day(selected_day)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Payments", f"{kpis['total_payments']:,}".replace(",", " "))
    c2.metric("Total Frauds", f"{kpis['total_frauds']:,}".replace(",", " "))
    c3.metric("Fraud Rate", f"{kpis['fraud_rate'] * 100:.2f} %")
    c4.metric("Fraud Amount", f"{kpis['fraud_amount']:.2f}")

    st.divider()

    # Table
    st.subheader("Payments (previous day)")
    df_day = load_transactions_for_day(selected_day, fraud_only=fraud_only)

    if df_day.empty:
        st.info("No transactions for selected date.")
    else:
        # Parse datetimes
        df_day["event_time"] = pd.to_datetime(df_day["event_time"], errors="coerce")
        df_day["ingested_at"] = pd.to_datetime(df_day["ingested_at"], errors="coerce")

        # Demo fallback: replace missing event_time by selected date
        fallback_dt = pd.to_datetime(selected_day)
        df_day["event_time"] = df_day["event_time"].fillna(fallback_dt)

        st.caption(f"{len(df_day)} transaction(s) for {selected_day}")
        st.dataframe(df_day, use_container_width=True, hide_index=True)

except Exception as e:
    st.error("Dashboard error.")
    st.exception(e)