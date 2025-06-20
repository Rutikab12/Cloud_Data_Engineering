from datetime import datetime
from contextlib import contextmanager

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.baseoperator import BaseOperator

# Operators; we need this to operate!
# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionCreatePipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionListPipelinesOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from airflow.models import Variable
from google.cloud import storage
from airflow.exceptions import AirflowFailException

import logging
from airflow.operators.bash import BashOperator
import pandas as pd
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from CustomVaultOperator import CustomVaultOperator
import CustomVaultClass
import gnupg
import time
import subprocess
from airflow.models import BaseOperator
from google.cloud import bigquery
import os

from reportlab.platypus import *

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dateutil.relativedelta import relativedelta
import datetime as dt

from airflow.models import XCom

pd.set_option('display.max_colwidth', None)


class IEDynamoIngestionOperator(BaseOperator):
    template_fields = (
        'period', 'batch_id','delimiter',
        'latest_key_name', 'dag_name_id', 'run_id','data_retrieval_path','data_archival_path','bq_table_name')

    def __init__(self, period: str, batch_id: str, latest_key_name: str, dag_name_id: str, run_id: str,
                 delimiter:str,data_retrieval_path:str,data_archival_path:str,bq_table_name:str,**kwargs) -> None:
        super().__init__(**kwargs)
        self.period = period
        self.batch_id = batch_id
        self.delimiter = delimiter
        self.latest_key_name = latest_key_name
        self.dag_name_id = dag_name_id
        self.run_id = run_id
        self.data_retrieval_path=data_retrieval_path
        self.data_archival_path=data_archival_path
        self.bq_table_name=bq_table_name
        #self.secret_pass_dec=secret_pass_dec

    def execute(self, context):
        # Function to move files in the GCS Buckets
        def mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
            storage_client = storage.Client()
            source_bucket = storage_client.bucket(bucket_name)
            source_blob = source_bucket.blob(blob_name)
            destination_bucket = storage_client.bucket(new_bucket_name)

            # copy to new destination
            new_blob = source_bucket.copy_blob(
                source_blob, destination_bucket, new_blob_name)
            # delete in old destination
            # source_blob.delete()

            print(f'File moved from {source_blob} to {new_blob_name}')
            
        data_retrieval_path=self.data_retrieval_path
        data_archival_path=self.data_archival_path
        bq_table_name=self.bq_table_name
        #secret_pass_dec=self.secret_pass_dec

        # SQL to fetch reffile decryption key
        #select "secretKeyName" from files where "originalFileName" like 'ie_workflow_config%' order by "createdAt" desc limit 1;
        '''def return_sql(filename1):
            sql = "select \"secretKeyName\" from files where \"originalFileName\" like " \
                  "'" + filename1 + "%' order by \"createdAt\" desc limit 1;"
            return sql'''
        #get files in error folder and move them to error_archive folder and show at frontend
        def move_error_file(data_retrieval_path,data_archival_path):
            storage_client = storage.Client()
            source_bucket = storage_client.bucket(Variable.get('ie_bucket'))
            blobs = client.list_blobs(Variable.get('ie_bucket'), prefix=data_retrieval_path)
            
            data_archival_path=data_archival_path+'error_files/'
            
            moved_files = []
            
            for blob in blobs:
                blob_name = str(blob.name)
        
                # Skip if not in error folder or not a CSV file
                if "error/" not in blob_name or not blob_name.lower().endswith('.csv'):
                    continue
                    
                # Generate archive filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                original_filename = blob_name.split('/')[-1]
                archive_filename = f"{original_filename.split('.')[0]}_{timestamp}.csv"
                archive_path = data_archival_path + archive_filename

                mv_blob(Variable.get('ie_bucket'), blob_name, Variable.get('ie_dag_bucket'),archive_path)
                
                moved_files.append(blob_name)
                logging.info(f"Moved error file {blob_name} to {archive_path}")
            
            if not moved_files:
                logging.info(f"No error CSV files found to archive under {data_retrieval_path}")
            else:
                logging.info(f"Archived {len(moved_files)} error CSV files")    
        

        def get_file_from_bucket(file_type,file_period, folder_name, data_retrieval_path):
            postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('ie_cloudsql_schema'))
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            gpg = gnupg.GPG()
            gpg.encoding = 'utf-8'
            #print("blob1")
            found_flag = False
            client = storage.Client()
            blobs = client.list_blobs(Variable.get('ie_bucket'), prefix=data_retrieval_path)
            
            if not blobs:
                raise AirflowFailException("Error : Please upload the file in bucket !!")
            else:
                for blob in blobs:
                    print("print new blob")
                    print(str(blob))
                    print(str(blob.name))
                    tmp_filename = str(blob.name)#tmp_filename:Dynamo/Inbound/Landing/Transactions/dynamo_transaction_file_2024_09.txt

                    if "error/" in tmp_filename:
                        print(f"skipping file in error folder: {tmp_filename}")
                        continue

                    print("tmp_filename: " + tmp_filename)
                    if tmp_filename.lower() is not None:
                        filename = tmp_filename

                        head, tail = os.path.split(filename)
                        print('filename: ' + filename)
                        print('head: ' + head) #Dynamo/Inbound/Landing/Transactions
                        print('tail: ' + tail) #dynamo_transaction_file_2024_09.txt
                        if 'csv' not in tail :
                            print("passing the loop")
                            continue

                        #data/ie/ie_dynamo_transaction_data_ingestion/dynamo_transaction_file_2024_09.txt
                        #folder_name is nothing but dag_name
                        destination_path = 'data/ie/' + folder_name + '/' + file_type + '.csv'
                        print("for loop dest path: "+destination_path)


                        mv_blob(Variable.get('ie_bucket'), filename, Variable.get('ie_dag_bucket'),
                                destination_path)

                        logging.info(
                            'File found for : ' + file_type + ' and for file_period: ' + file_period)

                        found_flag = True

                        break

                    print("filename :" + filename)
                #print("Ref file name :" + reffile_to_find)
                if filename is None:
                    raise AirflowFailException(
                        "Error : file_type: " + file_type + " Not Found !! for file_period: " + file_period)

                return found_flag, filename
         
        #fetch global variables---------------------------------------------------------------------------------------------------------
        file_period = self.period
        folder_name = self.dag_name_id
        #folder_name='ie_dynamo_transaction_data_ingestion'
        delimiter = self.delimiter
        data_retrieval_path=self.data_retrieval_path
        data_archival_path=self.data_archival_path
        file_type = 'dynamo_transaction_file'

        postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('ie_cloudsql_schema'))
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        gpg = gnupg.GPG()
        gpg.encoding = 'utf-8'

        dag_name_id = self.dag_name_id
        dag_run_id = self.run_id
        workflow_name = dag_name_id
        dag_run_id = dag_run_id
        
        #fetch error files
        move_error_file(data_retrieval_path,data_archival_path)

        # Fetch Transactions file
        get_file_from_bucket(file_type, file_period, folder_name, data_retrieval_path)
        
        client = bigquery.Client()
        dataset = Variable.get('ie_bq_staging_dataset')
        bq_project = Variable.get('ie_project_id')
        bigquery_table = bq_table_name
        
        logging.info("Deleting existing BQ Data from table: " + str(bigquery_table))

        try:
            QUERY = "Delete from `" + bq_project + "." + dataset + "." + str(bigquery_table) + "` WHERE true"
            logging.info('QUERY : ' + QUERY)
            query_job = client.query(QUERY)

        except Exception:
            logging.info('Query failed or table does not exist')
            # logging.info(Exception)
        
        #CLEAN HEADERS---------------------------------------------------------------------------------------------------------
        
        #decrypted_path: /home/airflow/gcs/data/ie/ie_dynamo_transaction_data_ingestion/dec/dynamo_transaction_file.txt'
        #destination_path: data/ie/ie_dynamo_transaction_data_ingestion/dynamo_transaction_file_2024_09.txt
        #file_name='/home/airflow/gcs/data/ie/' + dag_name_id + '/' + file_type + '.txt'
        
        outdir = '/home/airflow/gcs/data/ie/' + dag_name_id + '/modified/'
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        tail = file_type + '.txt'
        print("tail: "+tail)

        all_sheets = []
        
        sheets_dict = pd.read_csv('/home/airflow/gcs/data/ie/' + dag_name_id + '/' + file_type + '.csv',sep=delimiter,dtype=str)
        #sheets_dict.columns = [x.lower() for x in sheets_dict.columns]
        
        all_sheets = []
        sheets_dict.columns = [x.replace("\n", " ") for x in sheets_dict.columns.to_list()]
        sheets_dict = sheets_dict.rename(columns=lambda x: x.split('\n')[-1])
        sheets_dict = sheets_dict.loc[:, ~sheets_dict.columns.duplicated()].copy()
        all_sheets.append(sheets_dict)
        
        df = pd.concat(all_sheets)
        # df.fillna('')
        df = df.loc[:, ~df.columns.duplicated()].copy()
        df.reset_index(inplace=True, drop=True)
        #df['file_name'] = '/home/airflow/gcs/data/ie/' + dag_name_id + '/dec/' + file_type + '.txt'
        
        df.rename(columns=lambda x: x.strip())
        df.columns = df.columns.str.replace('%', 'percent')
        df.columns = [re.sub('[^A-Za-z0-9_]+', '_', str(c)) for c in df.columns]
        df.columns = df.columns.str.replace('__', '_')
        df.columns = [re.sub('_$', '', str(c)) for c in df.columns]
        df.fillna('nan')
        #print(df)
        cols = pd.Series(df.columns)

        # rename duplicate columns
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in
                                                             range(sum(cols == dup))]

        df.columns = cols
        df = df.replace(r'\n', ' ', regex=True)
        tail = os.path.splitext(tail)[0] + '.txt'
        df.to_csv(outdir + tail, index=None, sep='\t')
        print("file saved as txt in modified folder")

        ###TRIGGER DYNAMIC DF----------------------------------------------------------------------------------------------------####
        timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        batch_id = self.batch_id
        latest_key_name = self.latest_key_name
        
        file_directive = ''
        pl_name = 'ie_dynamo_txt_file_load'
        file_directive += 'parse-as-csv :body \'\\t\' true\n'
        file_directive += 'drop :body\n'
        file_directive += 'set-column batch_id ' + '\'' + batch_id + '\'\n'
        file_directive += 'set-column timestamp ' + '\'' + timestamp_value + '\'\n'
        file_directive += 'parse-as-datetime :timestamp \'yyyy-MM-dd HH:mm:ss\''
        
        #file_name use for generating final schema
        filepath = '/home/airflow/gcs/data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
        #final_file_name sent to DF pl
        filepath_gcs = 'gs://' + Variable.get(
                    'ie_dag_bucket') + '/data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
        file_cols = pd.read_csv(filepath, sep='\t').columns.tolist()
        file_cols.insert(0, "batch_id")
        file_cols.insert(1, "timestamp")
        appended_string = ''
        pre_string = '{\"type\":\"record\",\"name\":\"record\",\"fields\":['
        post_string = ']}'
         
        #make schema for pipeline in DF-------------------------------------------------------------------------------------------------
        for i, col in enumerate(file_cols):
            if file_cols[i] == 'timestamp':
                column = '{\"name\":\"timestamp\",\"type\":[{\"type\":\"string\",\"logicalType\":\"datetime\"},\"null\"]}'
            else:
                column = '{\"name\":\"' + file_cols[i] + '\",\"type\":[\"string\",\"null\"]}'

            if i == 0:  # first column in schema
                pre_string += column + ','
                appended_string += pre_string

            elif i == (len(file_cols) - 1):  # last column in schema
                end_string = column + post_string
                appended_string += end_string

            else:
                appended_string += column + ','

        
        #trigger pl operator------------------------------------------------------------------------------------------------------------
        trigger_pipeline = CloudDataFusionStartPipelineOperator(
            location=Variable.get('ie_pl_region'),
            pipeline_name=pl_name,
            instance_name=Variable.get('ie_df_instance'),
            success_states=['COMPLETED'],
            namespace=Variable.get('ie_df_namespace'),
            task_id='trigger_pipeline' + str(file_type),
            runtime_args={'system.profile.name': Variable.get('ie.system.profile.name'),
                          'filepath': filepath_gcs,
                          'output_schema': appended_string,
                          'project_id': Variable.get('ie_project_id'),
                          'recipe': file_directive,
                          'ie_bq_table_name': bigquery_table,
                          'ie_bucket': Variable.get('ie_bucket'),
                          'ie_bq_dataset': Variable.get('ie_bq_staging_dataset'),
                          'ie_bq_region': Variable.get('ie_bq_region')
                          },
            asynchronous=True,
            gcp_conn_id=Variable.get('gcp_conn_id'),
            pipeline_timeout=20 * 60,
        )
        try:
            trigger_pipeline.execute(dict())
        except:
            pass
            
        wait_task = PythonOperator(
            task_id="wait_task",
            python_callable=lambda: time.sleep(260),
            trigger_rule="all_success",
        )
        wait_task.execute(dict())
        
        #Check BQ Table after loading---------------------------------------------------------------------------------------------------
        count = 0
        query = "SELECT  count(1) as count FROM `" + bq_project + "." + dataset + "." + bigquery_table + "`"
        logging.info("Query: " + query)
        query_job = client.query(query)
        rows = query_job.result()
        i=bigquery_table

        for row in rows:
            count = row.count
            logging.info("Count is: " + str(count))

            if count >= 1:
                logging.info("Data Found for table: " + i)
                context['task_instance'].xcom_push(key=i, value=str(count))
                continue
            else:
                context['task_instance'].xcom_push(key=i, value=str(count))
                try:
                    modified_file = 'data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
                    modified_destination_path = data_archival_path + file_type + '_' + timestamp_value + '_' + dag_name_id + '.txt'
                    mv_blob(Variable.get('ie_dag_bucket'), modified_file, Variable.get('ie_bucket'),
                            modified_destination_path)
                    print("File moved from Modified to Archived Folder")
                except:
                    print("Unable to move files to Archive Folder")

                try:
                    client = storage.Client()
                    blobs = client.list_blobs(Variable.get('ie_bucket'), prefix=data_retrieval_path)
                    for blob in blobs:
                        if blob.name==data_retrieval_path.rstrip('/') + '/':
                            continue
                        #if "error/" in blob.name:
                            #continue
                        blob.delete()
                        print(f"Deleted: {blob.name}")
                    print(f"Deleted files from gcs retrieval path")
                except:
                    print("Unable to delete files from retrieval path")

                gcs_bucket = Variable.get('ie_bucket')
                dag_bucket = Variable.get('ie_dag_bucket')

                dag_path_to_delete = 'data/ie/' + dag_name_id + '/'

                delete_txt_files2 = f"gsutil -m rm gs://{dag_bucket}/{dag_path_to_delete}*"
                try:
                    subprocess.run(delete_txt_files2, shell=True, check=True)
                    self.log.info("deleted files and folder from dag bucket")
                except subprocess.CalledProcessError as e:
                    self.log.info(f"Error Deleting Files: {e}")
                    raise
                raise AirflowFailException(" No Data Found!!! for table..." + i)
                
        #Move & Delete files-----------------------------------------------------------------------------------------------
        
        try:
            modified_file='data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
            modified_destination_path = data_archival_path + file_type + '_' + timestamp_value + '_' + dag_name_id + '.txt'
            mv_blob(Variable.get('ie_dag_bucket'), modified_file, Variable.get('ie_bucket'),
                                    modified_destination_path)
            print("File moved from Modified to Archived Folder")
        except:
            print("Unable to move files to Archive Folder") 
       
        try:
            client = storage.Client()
            blobs = client.list_blobs(Variable.get('ie_bucket'), prefix=data_retrieval_path)
            for blob in blobs:
                #skip the folder itself
                if blob.name==data_retrieval_path.rstrip('/') + '/':
                    continue
                #if "error/" in blob.name:
                    #continue
                blob.delete()
                print(f"Deleted: {blob.name}")
            print(f"Deleted files from gcs retrieval path")
        except:
            print("Unable to delete files from retrieval path")
        
        gcs_bucket=Variable.get('ie_bucket')
        dag_bucket=Variable.get('ie_dag_bucket')
        
        dag_path_to_delete='data/ie/'+dag_name_id+'/'
               
        delete_txt_files2=f"gsutil -m rm gs://{dag_bucket}/{dag_path_to_delete}*"
        try:
            subprocess.run(delete_txt_files2,shell=True,check=True)
            self.log.info("deleted files and folder from dag bucket")
        except subprocess.CalledProcessError as e:
            self.log.info(f"Error Deleting Files: {e}")
            raise