from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
from airflow.models import Variable
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

def process_error_files(**context):
    """Process all error CSV files in the directory and load to BigQuery"""
    bucket_name = Variable.get('ie_bucket')
    prefix = '2025-24-06-11-02-08/'  # Update with your directory path or make it dynamic
    dataset_id = Variable.get('ie_bq_staging_dataset')
    table_id = 'error_files'  # Update with your table name
    
    client = storage.Client()
    bq_client = bigquery.Client()
    
    # Get all error files in the directory
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    error_files = [
        blob.name for blob in blobs 
        if blob.name.lower().endswith('.csv') 
        and 'errEXT1507' in blob.name  # Pattern to identify error files
    ]
    
    if not error_files:
        logging.info("No error files found to process")
        return
    
    # Process each file
    for file_path in error_files:
        try:
            # Extract filename for logging
            filename = os.path.basename(file_path)
            
            # Define BigQuery load job config
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Adjust if your files have headers
                autodetect=True,  # Or define your schema explicitly
                write_disposition='WRITE_APPEND'  # Append to existing table
            )
            
            # Construct the URI for the file
            uri = f"gs://{bucket_name}/{file_path}"
            
            # Load the file into BigQuery
            load_job = bq_client.load_table_from_uri(
                uri,
                f"{dataset_id}.{table_id}",
                job_config=job_config
            )
            
            load_job.result()  # Wait for job to complete
            
            logging.info(f"Successfully loaded {filename} into BigQuery")
            
            # Optional: Move processed file to archive
            # archive_path = f"archive/{file_path}"
            # mv_blob(bucket_name, file_path, bucket_name, archive_path)
            
        except Exception as e:
            logging.error(f"Failed to process {file_path}: {str(e)}")
            raise

with DAG(
    'process_error_files',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['error_processing']
) as dag:
    
    process_task = PythonOperator(
        task_id='process_error_files',
        python_callable=process_error_files,
        provide_context=True
    )