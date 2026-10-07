-- Runs once, on the first start of an empty data volume.
-- One database per tool, plus the platform's own database for the local environment.
CREATE DATABASE mlflow;
CREATE DATABASE airflow;
CREATE DATABASE fraud_local;
