# Near-Real-Time Fraud Detection Pipeline (Airflow + SageMaker)

Production-style data pipeline that detects fraudulent payments in near
real time using an **Airflow-orchestrated workflow** and a **machine
learning model deployed on AWS SageMaker**.

## Key Features

-   Near-real-time fraud detection pipeline
-   Airflow DAG running every minute
-   Idempotent ingestion into PostgreSQL
-   Real-time ML inference via AWS SageMaker
-   Email alerting when fraud is detected
-   Streamlit dashboard for daily monitoring

------------------------------------------------------------------------

# Tech Stack

### Data Engineering

-   Apache Airflow
-   PostgreSQL
-   Docker

### Machine Learning

-   XGBoost
-   AWS SageMaker

### Application / Visualization

-   Streamlit

### Language

-   Python

------------------------------------------------------------------------

# Architecture

```{=html}
<p align="center">
```
`<img src="Bloc%203%20-%20Sch%C3%A9ma%20infrastructure.png" width="800">`{=html}
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<em>`{=html}Fraud detection pipeline architecture`</em>`{=html}
```{=html}
</p>
```
  --------------------------------------------------------------------------
  Component                 Role            Location
  ------------------------- --------------- --------------------------------
  Airflow 2.8               Pipeline        Docker (local)
  (LocalExecutor)           orchestration   

  PostgreSQL                Storage of      Docker (local)
                            scored          
                            transactions    

  Streamlit                 Operational     Docker (local)
                            reporting       
                            dashboard       

  AWS SageMaker Endpoint    Real-time ML    AWS
                            inference       
  --------------------------------------------------------------------------

Local services are containerized with Docker.\
Model inference is performed through a **SageMaker endpoint invoked by
Airflow**.

------------------------------------------------------------------------

# Pipeline Overview

The Airflow DAG executes the following pipeline every minute.

## 1. Extract

-   Retrieve payment events from an external API simulating real-time
    transactions
-   Validate payload
-   Handle empty responses and API errors

## 2. Transform

-   Parse JSON payload
-   Clean and cast data types
-   Align features with model expectations
-   Apply preprocessing pipeline (`preprocess.joblib`)
-   Produce an inference-ready dataset

## 3. Score

-   Send transactions to the SageMaker endpoint using
    `boto3.invoke_endpoint`
-   Retrieve fraud probability
-   Apply configurable threshold (`FRAUD_THRESHOLD`) to flag fraud

## 4. Load

Insert scored transactions into the table `payments_scored`.

UPSERT strategy:

    ON CONFLICT (trans_num) DO UPDATE

This guarantees:

-   Pipeline idempotence
-   No duplicate transactions
-   Safe reprocessing

## 5. Alert

-   Send an email notification when fraud is detected

------------------------------------------------------------------------

# Airflow DAG

DAG file: `fraud_pipeline_dag.py`\
Schedule: **every minute**

The DAG orchestrates the full pipeline:

API ingestion → transformation → ML inference → database storage →
alerting.

------------------------------------------------------------------------

# Database Design

Database: `fraud_db`\
Main table: `payments_scored`

Key characteristics:

-   `trans_num` defined as **PRIMARY KEY**
-   UPSERT ingestion strategy
-   SQL reporting view: `v_daily_fraud_report`

------------------------------------------------------------------------

# Machine Learning Layer

The model was:

1.  Trained using **AWS SageMaker**
2.  Deployed behind a **real-time inference endpoint**

The repository includes:

    preprocess.joblib

This guarantees **reproducibility of the transformation pipeline**.

------------------------------------------------------------------------

# Streamlit Reporting

A dedicated Streamlit container (port **8501**) queries the SQL view
`v_daily_fraud_report`.

It provides:

-   Daily transaction monitoring
-   Visualization of detected fraud cases
-   Operational dashboard for fraud analysts

------------------------------------------------------------------------

# Running the Project Locally

## Requirements

-   Docker
-   Docker Compose
-   AWS credentials configured
-   SageMaker endpoint available

## Start the infrastructure

``` bash
docker compose up --build
```

## Access interfaces

Airflow UI:

    http://localhost:8080

Streamlit dashboard:

    http://localhost:8501

------------------------------------------------------------------------

# Design Principles

-   Pipeline-oriented architecture
-   Centralized orchestration via Airflow
-   Idempotent data ingestion
-   Clear separation of responsibilities
-   Containerized infrastructure
-   Production-compatible design

------------------------------------------------------------------------

# Author

Sandra Margot\
Data Engineering

Stack: **AWS SageMaker • XGBoost • Docker • Airflow • PostgreSQL •
Streamlit • Python**

------------------------------------------------------------------------

# Version française

## Contexte et objectif

Ce projet met en œuvre une **pipeline de détection de fraude orchestrée
par Airflow**, conçue pour fonctionner en quasi temps réel.

Exigences métier :

-   Notification immédiate en cas de fraude détectée
-   Consultation quotidienne des paiements et fraudes survenus la veille

L'objectif principal est la **conception et l'implémentation d'un DAG
Airflow robuste, idempotent et orienté production**.

------------------------------------------------------------------------

## Architecture

  --------------------------------------------------------------------------
  Composant                 Rôle            Localisation
  ------------------------- --------------- --------------------------------
  Airflow 2.8               Orchestration   Docker (local)
  (LocalExecutor)           du pipeline     

  PostgreSQL                Stockage des    Docker (local)
                            transactions    
                            scorées         

  Streamlit                 Reporting       Docker (local)
                            quotidien       

  AWS SageMaker Endpoint    Inférence temps AWS
                            réel du modèle  
  --------------------------------------------------------------------------

Les services locaux sont containerisés via Docker.

------------------------------------------------------------------------

## Pipeline

Le DAG Airflow s'exécute toutes les minutes et implémente les étapes
suivantes :

1.  **Extract** -- appel d'une API simulant des paiements temps réel\
2.  **Transform** -- nettoyage et alignement des features\
3.  **Score** -- appel de l'endpoint SageMaker\
4.  **Load** -- insertion idempotente dans PostgreSQL\
5.  **Alert** -- notification email en cas de fraude détectée

------------------------------------------------------------------------

## Auteur

Sandra Margot\
Data Engineering
