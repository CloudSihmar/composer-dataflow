# Apache Beam Dataflow Pipeline with Airflow DAG

This repository contains an Apache Beam pipeline script designed to process CSV data from Google Cloud Storage (GCS) and write the results to a BigQuery table. Additionally, it includes an Airflow DAG for scheduling and running the Dataflow job.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Apache Beam Pipeline Script](#apache-beam-pipeline-script)
  - [Default Values](#default-values)
  - [DataFlow Script](#DataFlow-Script)
- [Airflow DAG Script](#Airflow-DAG-Script)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Prerequisites

Before running the pipeline and the DAG, ensure you have the following prerequisites:
- A Google Cloud project with Dataflow, BigQuery, and GCS enabled.
- Google Cloud Storage bucket with input data. for example sandeep-apache is the bucket and input.csv is the file
- BigQuery dataset and table to write the results. Dataset: dataset_demo, Table: output, Schema: "products:STRING", "total_sales:INTEGER"

## Apache Beam Pipeline Script

### Default Values
Project: techlanders-internal
Temporary Location: gs://sandeep-apache/temp
Input File: gs://sandeep-apache/input.csv
Output Table: techlanders-internal.dataset_demo.output-sql
Region: us-central1
Zone: us-central1-c
Runner: DataflowRunner

#### DataFlow Script
```sh
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            dest='input',
            required=True,
            default='gs://sandeep-apache/input.csv',
            help='Input file to process.'
        )
        parser.add_argument(
            '--output',
            dest='output',
            required=True,
            default='techlanders-internal.dataset_demo.output',
            help='Output BQ table to write results to.'
        )

def run_pipeline(argv=None):
    pipeline_options = PipelineOptions(
        argv,
        runner='DataflowRunner',
        project='techlanders-internal',
        temp_location='gs://sandeep-apache/temp',
        region='us-central1',
        zone='us-central1-c'
    )
    custom_options = pipeline_options.view_as(CustomOptions)

    input_location = custom_options.input
    output_table = custom_options.output

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from GCS' >> beam.io.ReadFromText(input_location, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(lambda line: line.split(','))
            | 'Format to Dictionary' >> beam.Map(lambda fields: {
                'date': fields[0],
                'product': fields[1],
                'sales': int(fields[2]),
                'price': float(fields[3])
            })
            | 'Add Product Key' >> beam.Map(lambda elem: (elem['product'], elem))
            | 'Group by Product' >> beam.GroupByKey()
            | 'Calculate Total Sales' >> beam.Map(lambda group: {
                'product': group[0],
                'total_sales': sum(item['sales'] * item['price'] for item in group[1])
            })
            | 'Write to BQ' >> WriteToBigQuery(
                output_table,
                schema='product:STRING, total_sales:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
```

## Airflow DAG Script

```sh
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

```

