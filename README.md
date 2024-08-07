Apache Beam Dataflow Pipeline with Airflow DAG
This repository contains an Apache Beam pipeline script designed to process CSV data from Google Cloud Storage (GCS) and write the results to a BigQuery table. Additionally, it includes an Airflow DAG for scheduling and running the Dataflow job.

Prerequisites
Before running the pipeline and the DAG, ensure you have the following prerequisites:

Google Cloud SDK installed and authenticated.
Apache Beam installed in your Python environment.
Apache Airflow installed and configured.
Google Cloud Storage bucket with input data.
BigQuery dataset and table to write the results.
A Google Cloud project with Dataflow, BigQuery, and GCS enabled.
