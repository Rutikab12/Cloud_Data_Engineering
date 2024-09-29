from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from google.cloud.storage import Client


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def list_files(bucket_name,prefix,processed_prefix='processed/'):
    gcs_hook=GCSHook()
    files = gcs_hook.list(bucket_name, prefix=prefix)
    if files:
        #Move the file to the 'processed' subdirectory
        source_object = files[0]
        file_name = source_object.split('/')[-1]  # Get the file name
        destination_object = processed_prefix.rstrip('/') + '/' + file_name

        #Get the source blob
        storage_client = Client()
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_object)

        #Define the destination blob and create it with the same content as the source blob
        destination_blob = bucket.blob(destination_object)
        destination_blob.upload_from_string(source_blob.download_as_text())

        #Delete the source blob
        source_blob.delete()

        return destination_object
    else:
        return None
#food-orders-dev

with DAG('airflow_food_orders',
    description="{\"description\":\"**This workflow executes pipeline for food orders received daily and store it in BQ tables.}",
    default_args=default_args,
    schedule_interval='@daily',  #Run daily
    catchup=False,
    max_active_runs=1 #one run at a time
)as dag:
    
    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id='gcs_sensor',
        bucket='first-bucket-beam', #Add bucket name
        prefix='food_daily',
        mode='poke',
        poke_interval=60,  #Check every 60 seconds
        timeout=300  #Stop after 5 minutes if no file is found
    )
    
    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
        op_kwargs={'bucket_name': 'first-bucket-beam', 'prefix': 'food_daily'}, #Add bucket name
        do_xcom_push=True,  #This will push the return value of list_files to XCom
    )
    
    beamtask = BeamRunPythonPipelineOperator(
        task_id='beam_task',
        runner='DataflowRunner',
        py_file='gs://us-east1-food-orders-dev-d1d6ceed-bucket/food_orders_beam.py',
        pipeline_options={
            "input": 'gs://first-bucket-beam/{{ task_instance.xcom_pull("list_files") }}', #Add bucket name
            #add other pipeline options if needed
        },
        py_options=[],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='food_orders_processing_job',
            project_id='pysparkone', #Add project-id
            location='us-south1', #Add region
        ),
    )
    
    gcs_sensor >> list_files_task >> beamtask


