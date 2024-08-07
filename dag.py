# Import statement
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

# Define yesterday value for setting up start for DAG
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG main definition
with DAG(dag_id='dataflow_python_operator_example',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:
    
    # Dummy Start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Dataflow batch job process task
    dataflow_batch_process = DataFlowPythonOperator(
        task_id='dataflow_batch_process',
        py_file='gs://sandeep-apache/dataflow.py',  # Update with the GCS path to your script
        options={
            'input': 'gs://sandeep-apache/input.csv',  # Update with the input file location in GCS
            'output': 'techlanders-internal.dataset_demo.output'  # Update with your BigQuery table specification
        },
        dataflow_default_options={
            'project': 'techlanders-internal',  # Update with your GCP project ID
            "staging_location": "gs://sandeep-apache/staging",  # Update with the staging location in GCS
            "temp_location": "gs://sandeep-apache/temp"  # Update with the temp location in GCS
        },
        dag=dag) 
        
    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )   

# Setting up Task dependencies using Airflow standard notations        
start >> dataflow_batch_process >> end
