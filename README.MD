# Détection Automatique de Fraude – Data Pipeline en Production

## Objectif du projet

Ce projet met en place une infrastructure permettant de détecter des transactions frauduleuses en quasi temps réel.

Exigences métier :
- Être notifié dès qu’une fraude est détectée
- Chaque matin, consulter l’ensemble des paiements et fraudes survenus la veille

L’accent est volontairement mis sur la **Data Pipeline** et l’infrastructure, et non sur l’algorithme de Machine Learning.

---

## Architecture

L’infrastructure repose sur les composants suivants :

- **Airflow 2.8.3 (LocalExecutor)** – Orchestration de la pipeline
- **PostgreSQL (`fraud_db`)** – Stockage des données métier
- **Streamlit** – Interface de reporting quotidien
- **AWS SageMaker Endpoint** – Inférence du modèle en temps réel

Airflow, PostgreSQL et Streamlit sont containerisés via Docker.

La séparation des responsabilités est claire :
- Orchestration : Airflow
- Stockage : PostgreSQL
- Inférence : SageMaker
- Visualisation : Streamlit

---

## Couche Machine Learning

Le modèle a été entraîné sur AWS SageMaker et déployé derrière un endpoint.

IMPORTANT : Pour des raisons de coût, l’endpoint n’est pas maintenu en permanence en service (mais il peut l'être sur demande auprès de l'auteur)  
Cependant, l’infrastructure est pleinement conçue pour fonctionner en production avec inférence temps réel.

---
 
## Data Pipeline (Livrables\Source code\fraud_detection\airflow)

Le DAG `fraud_pipeline_dag.py` s’exécute **toutes les minutes** et réalise les étapes suivantes :

1. **Extract**  
   Appel d’une API externe de paiements temps réel.

2. **Transform**  
   Prétraitement et feature engineering (preprocess.joblib dans Livrables\Source code\fraud_detection\fraud-pipeline\artifacts)

3. **Score**  
   Appel à un endpoint SageMaker pour obtenir la probabilité de fraude.

4. **Load**  
   Insertion des transactions scorées dans la table `payments_scored`.

5. **Alert**  
   Envoi d’un email lorsqu’une fraude est détectée.

---

## Conception de la base de données

- Base : `fraud_db`
- Table principale : `payments_scored`
- `trans_num` est défini comme **Primary Key**
- Les insertions utilisent `ON CONFLICT (trans_num) DO UPDATE`
Ce choix garantit :
- L’idempotence de la pipeline
- L’absence de doublons
- La robustesse en cas de reprocessing

Une vue SQL `v_daily_fraud_report` permet de consulter les paiements et fraudes de la veille.

---

## Reporting Streamlit (Livrables\Source code\fraud_detection\fraud-dashboard)

Un conteneur Streamlit séparé (port 8501) lit la vue `v_daily_fraud_report` afin de fournir un tableau de bord quotidien.

Cela répond directement au besoin métier de suivi journalier.

---

## Principes de conception

- Approche **Pipeline-first**
- Infrastructure containerisée
- Séparation claire des responsabilités
- Ingestion idempotente
- Monitoring temps réel + reporting quotidien
- Architecture orientée production

---

## Conclusion

Ce projet démontre la mise en production d’un système de détection de fraude en mettant l’accent sur :

- L’ingestion fiable des données
- L’orchestration automatisée
- Le stockage cohérent
- L’alerte en cas d’anomalie
- Le reporting métier

L’algorithme de Machine Learning est intégré comme composant modulaire.

---

## Auteur
Sandra MARGOT
Formation : Lead Data Science & Engineering — Certification Jedha
Projet : Automatic Fraud Detection (ETL with Airflow)
Stack : AWS SageMaker • XGBoost • Docker • PostgreSQL • Airflow • Python