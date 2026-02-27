# Détection Automatique de Fraude -- Pipeline Airflow en environnement quasi temps réel

## 1. Contexte et objectif

Ce projet met en œuvre une **pipeline de détection de fraude orchestrée
par Airflow**, conçue pour fonctionner en quasi temps réel.

Les exigences métier sont les suivantes :

-   Être notifié immédiatement en cas de fraude détectée\
-   Consulter chaque matin l'ensemble des paiements et fraudes survenus
    la veille

L'objectif principal de l'exercice est la **conception et
l'implémentation d'un DAG Airflow robuste, idempotent et orienté
production**.\
La partie Machine Learning est fonctionnelle mais volontairement
secondaire dans cet exercice.

------------------------------------------------------------------------

## 2. Architecture globale

L'infrastructure repose sur une séparation claire des responsabilités :

  ------------------------------------------------------------------------
  Composant                Rôle             Localisation
  ------------------------ ---------------- ------------------------------
  Airflow 2.8              Orchestration du Docker (local)
  (LocalExecutor)          pipeline         

  PostgreSQL               Stockage des     Docker (local)
                           transactions     
                           scorées          

  Streamlit                Reporting        Docker (local)
                           quotidien        

  AWS SageMaker Endpoint   Inférence temps  AWS
                           réel du modèle   
  ------------------------------------------------------------------------

Les composants locaux sont containerisés via Docker.\
L'inférence est réalisée via un endpoint AWS SageMaker appelé par
Airflow.

Le schéma d'architecture du projet est disponible dans le repository.

------------------------------------------------------------------------

## 3. Le DAG Airflow -- Élément central du projet

DAG : `fraud_pipeline_dag.py`\
Fréquence : exécution toutes les minutes

Le DAG implémente les étapes suivantes :

### 1. Extract

-   Appel d'une API externe simulant des paiements temps réel\
-   Validation du payload\
-   Gestion des erreurs et cas vides

### 2. Transform

-   Parsing JSON\
-   Nettoyage et casting des types\
-   Alignement des features avec celles du modèle\
-   Application du pipeline de preprocessing (`preprocess.joblib`)\
-   Production d'un dataset prêt pour l'inférence

### 3. Score

-   Appel HTTPS à l'endpoint SageMaker (boto3 -- `invoke_endpoint`)\
-   Récupération de la probabilité de fraude\
-   Calcul du flag fraude selon un seuil configurable
    (`FRAUD_THRESHOLD`)

### 4. Load

-   Insertion dans la table `payments_scored`\

-   Utilisation de :

        ON CONFLICT (trans_num) DO UPDATE

    Ce choix garantit :

    -   L'idempotence du pipeline\
    -   L'absence de doublons\
    -   La robustesse en cas de reprocessing

### 5. Alert

-   Envoi d'un email en cas de fraude détectée

------------------------------------------------------------------------

## 4. Conception base de données

Base : `fraud_db`\
Table principale : `payments_scored`

Caractéristiques :

-   `trans_num` défini comme PRIMARY KEY\
-   Stratégie UPSERT\
-   Vue SQL : `v_daily_fraud_report`

Cette vue permet de répondre directement à l'exigence métier de
reporting quotidien (J-1).

------------------------------------------------------------------------

## 5. Couche Machine Learning

Le modèle a été :

1.  Entraîné sur AWS SageMaker\
2.  Déployé derrière un endpoint temps réel

Pour des raisons de coût, l'endpoint n'est pas maintenu actif en
permanence.\
Cependant, l'architecture est conçue pour fonctionner en production avec
inférence temps réel.

Le fichier `preprocess.joblib` est versionné dans ce repository afin de
garantir la reproductibilité du pipeline de transformation.

Les datasets complets d'entraînement ne sont pas inclus dans le
repository pour des raisons de taille.

------------------------------------------------------------------------

## 6. Reporting Streamlit

Un conteneur Streamlit indépendant (port 8501) interroge la vue
`v_daily_fraud_report`.

Il permet :

-   La consultation des transactions de la veille\
-   La visualisation des fraudes détectées\
-   Un support opérationnel quotidien

------------------------------------------------------------------------

## 7. Reproductibilité et configuration

Le projet inclut :

-   `airflow/.env.example`
-   `fraud-dashboard/.env.example`

Ces fichiers décrivent les variables d'environnement nécessaires :

-   Connexion PostgreSQL\
-   Paramètres AWS\
-   Seuil de fraude\
-   Paramètres SMTP

Pour exécution locale : - Docker et Docker Compose requis\
- Credentials AWS configurés\
- Endpoint SageMaker disponible

Le jury n'a pas besoin de relancer l'infrastructure complète ; le
repository documente néanmoins les dépendances externes.

------------------------------------------------------------------------

## 8. Principes de conception

-   Architecture orientée pipeline\
-   Orchestration centralisée via Airflow\
-   Idempotence garantie\
-   Séparation claire des responsabilités\
-   Infrastructure containerisée\
-   Design compatible production

------------------------------------------------------------------------

## 9. Conclusion

Ce projet démontre :

-   La conception d'un DAG Airflow robuste\
-   L'intégration d'un modèle ML déployé sur AWS\
-   Une ingestion fiable et idempotente\
-   Un mécanisme d'alerte temps réel\
-   Un reporting quotidien répondant aux exigences métier

L'accent a été mis sur la qualité de l'orchestration et la cohérence de
l'infrastructure.

------------------------------------------------------------------------

## Auteur

Sandra Margot\
Lead Data Science & Engineering --- Certification Jedha\
Stack : AWS SageMaker • XGBoost • Docker • Airflow • PostgreSQL •
Streamlit • Python
