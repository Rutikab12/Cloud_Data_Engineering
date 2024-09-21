from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.models import Variable
from google.cloud import storage
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

import logging
import random
from airflow.operators.bash import BashOperator
import pandas as pd
import glob
import os
import re
import shutil
import ast
import collections
import numpy as np
import json
import time

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from CustomVaultOperator import CustomVaultOperator
import CustomVaultClass
import gnupg

from ie.IEExcelGenerateOperator import IEExcelGenerateOperator
from ie.ieactretingestionoperator import ieactretingestionoperator
from ie.IEWorkflowSupportingFileOperator import IEWorkflowSupportingFileOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def start(**kwargs):
    task_instance = kwargs['ti']
    dag_id = kwargs['dag_run'].dag_id
    dag_run_id = kwargs['dag_run'].run_id
    task_instance.xcom_push(key='dag_id_value', value=dag_id)
    task_instance.xcom_push(key='dag_run_id_value', value=dag_run_id)


# Derive directory and timestamp value to save the output file in GCS bucket.
def get_runtime_values(**kwargs):
    task_instance = kwargs['ti']
    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_ie', schema=Variable.get('cloudsql_schema_ie'))
    directory_value = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    sql1 = kwargs['get_batch_id']
    sql2 = kwargs['update_batch_id']
    pg_hook.run(sql2)
    records = pg_hook.get_records(sql=sql1)
    batch_id = str(records[0][0])
    logging.info("batch_id : " + batch_id)

    CustomBatchName = "ie_franchise_act_ret_calculation"
    logging.info("CustomBatchName : " + CustomBatchName)


    task_instance.xcom_push(key='directory', value=directory_value)
    task_instance.xcom_push(key='batch_id', value=records[0][0])
    task_instance.xcom_push(key='CustomBatchName', value=CustomBatchName)
    task_instance.xcom_push(key='timestamp_value', value=timestamp_value)

# Validation of DAG parameters and DAG will fail if required parameters are not provided.
def parameters_check(**kwargs):
    task_instance = kwargs['ti']
    required_params = ["PERIOD"]

    for key in kwargs:
        value = kwargs[key]
        logging.info("The KEY is : " + str(key))
        if key in required_params:
            logging.info("key: " + str(key) + " data type : " + str(type(value)))
            if len(str(value)) == 0 or (str(value) == 'None'):
                raise AirflowFailException(str(key) + " Parameter is null.")
                
                
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

    print(f'File copied from {source_blob} to {new_blob_name}')
      
    
def get_aead_decryption_keys(**kwargs):
    task_instance = kwargs['ti']
    Period = kwargs['Period']
    source_pay = kwargs['source_value']
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('cloudsql_schema_ie'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    def return_sql(source):
        sql = "select distinct \"secretkey\" from ie_aead_key_info where source ='" + source + "';"
        return sql

    sql_to_run=return_sql(source_pay)
    logging.info("AEAD INFO SQL: " + sql_to_run)
    cursor.execute(sql_to_run)
    result = cursor.fetchone()
    decrypt_key_name=source_pay + "_aead_key"
    if result is None:
        logging.info(decrypt_key_name + " : " + 'NONE!!! setting default key')
        logging.info("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
        task_instance.xcom_push(key=decrypt_key_name, value='None')
    else:
        task_instance.xcom_push(key=decrypt_key_name, value=str(result[0]))

    
# SQL to fetch reffile decryption key
def return_sql(filename):
    sql = "select \"secretKeyName\" from files where \"filePath\" like " \
          "'%" + filename + "' order by \"createdAt\" desc limit 1;"
    return sql


# Function to get reference files from approved folder and decrypt
def get_rate_file(**kwargs):
    file_type = 'ie_rates_template_vbu_aa'
    file_period = kwargs['PERFORMANCE_PERIOD']
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('cloudsql_schema_ie'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    client = storage.Client()
    blobs = client.list_blobs(Variable.get('ie_bucket'), prefix='refFiles/approved')
    file_type = file_type.replace(' ', '_')
    reffile_to_find = file_type + '_' + file_period
    filename = None

    if reffile_to_find == "_" + file_period or reffile_to_find == file_type + "_" or reffile_to_find == "_":
        raise AirflowFailException("Error : Please Enter file type or file period for search !!")
    else:
        for blob in blobs:
            tmp_filename = str(blob.name)

            # print("tmp_filename:" + tmp_filename)
            if tmp_filename.lower().find(reffile_to_find.lower()) != -1:
                filename = tmp_filename
                task_instance = kwargs['ti']

                sql_to_run = return_sql(filename)
                logging.info("AEAD INFO SQL " + filename + " : " + sql_to_run)
                cursor.execute(sql_to_run)
                result = cursor.fetchone()
                # head, tail = os.path.split(file_list[i])
                decrypt_key_name = 'DEC_KEY_' + file_type
                if result is None:
                    # print(decrypt_key_name + " : " + 'NONE!!! setting default key')
                    # task_instance.xcom_push(key="DECRYPT_KEY" + str(i+1), value='DEFAULT_KEY_VAL')
                    print("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
                    raise AirflowFailException(decrypt_key_name + " key not found.")
                else:
                    print(decrypt_key_name + " : " + str(result[0]))  # Security Breach remove this

                    vault_key = str(result[0])
                    vault_addr = Variable.get('vault_addr')
                    vault_role = Variable.get('vault_role_ie')
                    vault_secret = Variable.get('vault_path_ie')
                    vault_secret_key = vault_key
                    vault_secret_pass = CustomVaultClass.execute(vault_addr, vault_role, vault_secret,
                                                                 vault_secret_key)

                    head, tail = os.path.split(filename)
                    destination_path = 'data/ie/ie_act_ret_file/dest/' + tail

                    decrypted_path = '/home/airflow/gcs/data/ie/ie_act_ret_file/'
                    if not os.path.exists(decrypted_path):
                        os.makedirs(decrypted_path)

                    decrypted_path_final = decrypted_path + 'ie_act_ret_file_ie_act_ret_file.xlsx'

                    mv_blob(Variable.get('ie_bucket'), filename, Variable.get('ie_dag_bucket'),
                            destination_path)

                    with open('/home/airflow/gcs/' + destination_path, 'rb') as f:
                        status = gpg.decrypt_file(f, output=decrypted_path_final, passphrase=vault_secret_pass)
                        print("STATUS OK ? " + str(status.ok))
                        print("STDERR: " + str(status.stderr))

                    break

        print("filename :" + filename)
        print("Ref file name :" + reffile_to_find)
        if filename is None:
            raise AirflowFailException("Error : File Not Found !!")


# Function to get rate and trans key before ingestion
def key_load_as_parameter(**kwargs):
    task_instance = kwargs['ti']
    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'
    

    dag_id = kwargs['dag_run'].dag_id
    print("DAG ID : " + dag_id)
    dag_run_id = kwargs['dag_run'].run_id
    print("DAG run ID : " + dag_run_id)
    workflow_name = dag_id
    dag_run_id = dag_run_id
    
    #read sheet=key_configuration and generate keys
    workflow_config_path = '/home/airflow/gcs/data/ie/ie_act_ret_file/ie_act_ret_file_ie_act_ret_file.xlsx'
    expected_files_df = pd.read_excel(workflow_config_path,sheet_name='Key_Configuration_AA')

    for index, row in expected_files_df.iterrows():
        
        #create rate key
        rate_key = row['rate_key']        
        
        rate_key = rate_key.replace(",", "+")
        rate_key = rate_key.lower()
        logging.info('rate_key : ' + rate_key)
        
        #create trans key
        trans_key = row['trans_key']       
               
        trans_key = trans_key.replace(",", "+")
        trans_key = trans_key.lower()
        logging.info('trans_key : ' + trans_key)
    
    #read sheet=rate_config and create lists of attributes
    rate_files_df = pd.read_excel(workflow_config_path,sheet_name='Rate_Config_AA')
    rate_attr_list=rate_files_df['Rate_attr_name'].tolist()
    #print(rate_attr_list)
    trans_attr_list=rate_files_df['Transaction_attr_name'].tolist()
    #print(trans_attr_list)
    
    #find the index of device_ordered in trans_list
    index_of_trans_attr=[x.lower() for x in trans_attr_list]
    index_of_trans_attr=index_of_trans_attr.index("device_ordered")
    print(index_of_trans_attr)
    
    #using index of trans_list to get field name of rate_attr
    name_of_rate_attr=rate_attr_list[index_of_trans_attr]
    name_of_rate_attr=name_of_rate_attr.lower()
    print(name_of_rate_attr)
    
    #find index of add_on_key in trans_list
    index_of_addon_key=[x.lower() for x in trans_attr_list]
    index_of_addon_key=index_of_addon_key.index("add_on_key")
    print(index_of_addon_key)
    
    #using index of trans_list to find add_on_key field
    name_of_addon_field=rate_attr_list[index_of_addon_key]
    name_of_addon_field=name_of_addon_field.lower()
    print(name_of_addon_field)

    task_instance.xcom_push(key='rate_key', value=str(rate_key))
    task_instance.xcom_push(key='trans_key', value=str(trans_key))
    task_instance.xcom_push(key='name_of_rate_attr', value=str(name_of_rate_attr))
    task_instance.xcom_push(key='name_of_addon_field', value=str(name_of_addon_field))
    #task_instance.xcom_push(key='attr_of_event_type_id', value=str(attr_of_event_type_id))
    task_instance.xcom_push(key='expected_files_df', value=str(expected_files_df.to_dict('dict')))
    
def load_aead_key_info_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('cloudsql_schema_ie'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    Period = kwargs['period']
    key_name = kwargs['key_name']
    source_pay = kwargs['source_value']

    def return_delete_sql(source):
        sql = "delete from ie_aead_key_info where \"source\"='" + source + "';"
        return sql


    del_sql = return_delete_sql(source_pay)
    logging.info("DELETE AEAD INFO SQL: " + del_sql)
    postgres_hook.run(del_sql)

    insert_sql = 'insert into ie_aead_key_info ("secretkey","source") values (\'' + key_name + '\',\'' + source_pay + '\');'

    logging.info("INSERT AEAD INFO SQL: " + insert_sql)
    cursor.execute(insert_sql)

    conn.commit()
    conn.close()
    
#showing calculation output files in Calculation_Output on Frontend
#first update the batch table with latest batch_id
def load_batches_table(**kwargs):
    batch_id = kwargs['batch_id']
    timestamp_value = kwargs['timestamp_value']
    payment_period = kwargs['payment_period']
    batch_name = kwargs['batch_name']

    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_ie', schema=Variable.get('cloudsql_schema_ie'))

    sql1 = "select id  from \"approvalStatus\" where lower(\"name\")= lower('New');"
    logging.info(sql1)
    records = pg_hook.get_records(sql1)
    approval_status_id = records[0][0]
    logging.info("approvalStatusId:- " + str(approval_status_id))
    logging.info("batch_name:- " + batch_name)

    sql2 = "select id  from \"batch_type\" where lower(\"type\")= lower('payment');"
    logging.info(sql2)
    records = pg_hook.get_records(sql2)
    batch_type_id = records[0][0]
    logging.info("Batch_type_id:- " + str(batch_type_id))

    # is_backup and updated_at column update condition check and updating

    sql5 = "update batches set is_backup=True, updated_at='" + timestamp_value + "' where batch_name = '" + \
           batch_name + "' AND batches.period='" + payment_period + \
           "' AND \"approvalStatusId\" IN (select \"approvalStatus\".id from \"approvalStatus\" where " \
           "lower(\"approvalStatus\".name) IN ('new', 'awaiting approval', 'rejected'));commit;"
    logging.info("update sql :- " + sql5)
    pg_hook.run(sql5)

    sql3 = "insert into batches(period, batch_name, batch_id, created_at, updated_at, is_active, \"approvalStatusId\", \"isApproved\", batch_type_id) " \
           "values('" + payment_period + "','" + batch_name + "'," + str(
        batch_id) + ",'" + timestamp_value + "','" + timestamp_value + "','t'," + str(
        approval_status_id) + ",'0'," + str(batch_type_id) + ");"
    logging.info(sql3)
    pg_hook.run(sql3)


def load_files_and_batchfiles_table(**kwargs):
    batch_id = kwargs['batch_id']
    timestamp_value = kwargs['timestamp_value']
    directory_value = kwargs['directory_value']
    gpg_vault_secret_key = kwargs['gpg_vault_secret_key']

    # Calculation REPORT files and batch files:
    file_path = "gs://" + Variable.get('ie_bucket') + "/" \
                + "Calculation_output/ie_act_ret_calculation/" + directory_value

    prefix_file_path = "Calculation_output/ie_act_ret_calculation/" + directory_value

    pg_conn = PostgresHook(postgres_conn_id='cloudsql_conn_ie', schema=Variable.get('cloudsql_schema_ie')).get_conn()

    client = storage.Client()

    blobs = client.list_blobs(Variable.get('ie_bucket'),
                              prefix=prefix_file_path)

    for blob in blobs:
        f = str(blob.name).split("/")[-1]
        if len(f) != 0:
            logging.info('Filename is : ' + str(f))

            sql1 = "insert into files(\"fileName\", \"originalFileName\", \"filePath\",\"createdAt\",\"updatedAt\",\"secretKeyName\") " \
                   "values('" + f + "','" + f + "','" + prefix_file_path + '/' + f + "','" + timestamp_value + "','" \
                   + timestamp_value + "','" + gpg_vault_secret_key + "') RETURNING id;"
            logging.info("files insert data sql:- " + sql1)

            cursor = pg_conn.cursor()
            cursor.execute(sql1)
            file_id = cursor.fetchone()[0]
            pg_conn.commit()
            logging.info("id generated for files: " + str(file_id))

            # inserting data into batchFiles table
            sql2 = "insert into \"batchFiles\"(\"fileId\", \"createdAt\", \"batchId\") " \
                   "values (" + str(file_id) + ",'" + timestamp_value + "'," + str(batch_id) + ");"

            logging.info("batchFiles insert data sql:- " + sql2)

            cursor.execute(sql2)
            pg_conn.commit()
            cursor.close()
            
#IE_Franchise_Late_Insurance_Securenet
#function to get latest and approved batch_id from batch table in postgres
def get_latest_batch(**kwargs):
    task_instance = kwargs['ti']
    payment_period = kwargs['payment_period']
    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_ie', schema=Variable.get('cloudsql_schema_ie'))
    
    sql="select max(batch_id) from \"batches\" where lower(\"batch_name\")='ie_franchise_late_insurance_securenet' AND \"period\"='" + \
        payment_period + "' AND \"isApproved\"='1'";
    logging.info(sql)
    records = pg_hook.get_records(sql)
    batch_id=records[0][0]
    logging.info("approved batch_id:- " + str(batch_id))
    approved_batch_id=batch_id
    print(approved_batch_id)
    
    task_instance.xcom_push(key='approved_batch', value=str(approved_batch_id))


with DAG(
        'ie_franchise_acquisition_and_retention_calculation',
        default_args=default_args,
        description="{\"name\":\"Franchise Acquisition and Retention Calculation\",\"description\":\"**This workflow executes pipeline for activation and retention calculation.**\\n**It takes below inputs:-**\\n1. Read Rate file \\n2. Workflow Config file \\n\\n**This workflow has below tasks:-**\\n1. Get parameter values\\n2. Basis parameter values fetch reference/ transaction files from Cloud Storage.\\n3. Trigger Pipeline for activation_retention_calculation in Cloud Data Fusion.\\n4. Generate Reports on Cloud Storage in excel format and in raw data batches.\",\"fields\":[{\"name\":\"period\",\"type\":\"date\",\"label\":\"Period\",\"format\":\"yyyy_MM\"}]}",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        schedule_interval=None,
        max_active_runs=1,
        tags=['ie', 'ie_calculation'],
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=start,
        provide_context=True,
    )

    PERIOD = '{{ dag_run.conf.get("period") }}'
    FILE_PERIOD = '{{ dag_run.conf.get("period") }}'

    # Derive directory and timestamp value to save the output file in GCS bucket.
    get_runtime_values = PythonOperator(
        task_id='get_runtime_values',
        python_callable=get_runtime_values,
        provide_context=True,
        op_kwargs={
            'get_batch_id': 'select max(batch_id) from latest_batch_id;',
            'update_batch_id': 'update latest_batch_id set batch_id=batch_id+1 where true;commit;',
            'period': '{{ dag_run.conf.get("period") }}'
        },
        pool='batch_id_pool'
    )

    # Validation of DAG parameters and DAG will fail if required parameters are not provided.
    parameters_check = PythonOperator(
        task_id='parameters_check',
        python_callable=parameters_check,
        op_kwargs={
            'PERIOD': '{{ dag_run.conf.get("period") }}'
        }
    )


    fetch_latest_encryption_key = CustomVaultOperator(
        task_id='fetch_latest_encryption_key',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_ie'),
        vault_secret=Variable.get('vault_path_ie'),
        vault_secret_key="latest_key_name"
    )
    vault_secret_key_name = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_key',key='vault_value')}}"
    gpg_vault_secret_key = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_key',key='vault_value')}}"

    fetch_latest_encryption_pass = CustomVaultOperator(
        task_id='fetch_latest_encryption_pass',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_ie'),
        vault_secret=Variable.get('vault_path_ie'),
        vault_secret_key=vault_secret_key_name
    )
    secret_pass = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_pass',key='vault_value')}}"
    
    #task for reading rate file
    get_rate_file = PythonOperator(
            task_id='get_rate_file',
            python_callable=get_rate_file,
            op_kwargs={
            'PERFORMANCE_PERIOD': '{{ dag_run.conf.get("period") }}',
        }
    )
    
    #task for concatenate keys from rate file
    key_load_as_parameter = PythonOperator(
        task_id='key_load_as_parameter',
        python_callable=key_load_as_parameter,
        op_kwargs={
            'PERFORMANCE_PERIOD': '{{ dag_run.conf.get("period") }}',
        }
    )


    '''# ingestion operator
    external_dag_id = "{{task_instance.xcom_pull(task_ids='start', key='dag_id_value')}}"
    external_dag_run_id_value = "{{task_instance.xcom_pull(task_ids='start',key='dag_run_id_value')}}"

    get_required_files = ieactretingestionoperator(
        task_id='get_required_files',
        period='{{ dag_run.conf.get("period") }}',
        batch_id="{{task_instance.xcom_pull(task_ids='get_runtime_values',key='batch_id')}}",
        latest_key_name=gpg_vault_secret_key,
        dag_name_id=external_dag_id,
        run_id=external_dag_run_id_value
    )'''
    
    #task for getting latest approved batch_id from batches table
    get_latest_batch = PythonOperator(
        task_id="get_latest_batch",
        python_callable=get_latest_batch,
        op_kwargs={
            'payment_period': '{{ dag_run.conf.get("period") }}'
        },
        provide_context=True,
        trigger_rule="all_success"
    )
    
    
    trigger_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline",
        location=Variable.get('ie_pl_region'),
        pipeline_name='ie_franchise_acquisition_and_retention_calculation',
        instance_name=Variable.get('ie_df_instance'),
        success_states=['COMPLETED'],
        namespace=Variable.get('ie_df_namespace'),
        runtime_args={'PERIOD': PERIOD,
                      'bucket-name': Variable.get('ie_bucket'),
                      'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
                      'project_id': Variable.get('ie_project_id'),
                      'bq_region': Variable.get('ie_bq_region'),
                      'gcs_region': Variable.get('ie_gcs_region'),
                      'staging_dataset': Variable.get('ie_bq_staging_dataset'),
                      'gold_dataset':Variable.get('ie_bq_gold_dataset'),
                      'system.profile.name': Variable.get('ie.system.profile.name'),
                      'DEC_KEY_GPG': vault_secret_key_name,
                      'vault_path': Variable.get('vault_path_ie'),
                      'PERIOD': PERIOD,
                      'source': "ie_franchise_acq_ret_calc",
                      'current_run_batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='batch_id')}}",
                      'timestamp_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='timestamp_value')}}",
                      'rate_key': "{{task_instance.xcom_pull(task_ids='key_load_as_parameter',key='rate_key')}}",
                      'name_of_rate_attr': "{{task_instance.xcom_pull(task_ids='key_load_as_parameter',key='name_of_rate_attr')}}",
                      'name_of_addon_field': "{{task_instance.xcom_pull(task_ids='key_load_as_parameter',key='name_of_addon_field')}}",
                      'trans_key': "{{task_instance.xcom_pull(task_ids='key_load_as_parameter',key='trans_key')}}",
                      'approved_batch': "{{task_instance.xcom_pull(task_ids='get_latest_batch',key='approved_batch')}}"
                      },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="all_success"
    )
    
    #excel file with sheets for calc output and summary outputs
    report_generation = IEExcelGenerateOperator(
        task_id="report_generation",
        raw_files_path= "gs://" + Variable.get(
            'ie_bucket') + "/IE_ACT_RET_CALCULATION/Reports/{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
        tab_list= "DETAILED_REPORT",
        decimal_columns="None",
        final_excel_path= "gs://" + Variable.get(
            'ie_bucket') + "/Calculation_output/ie_act_ret_calculation/{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
        final_excel_name= "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" +
                            "_IE_ACT_RET_CALC_" + '{{ dag_run.conf.get("period") }}' + "_REPORT.xlsx",
        gpg_vault_secret_key=gpg_vault_secret_key
    )
    
    get_aead_decryption_keys = PythonOperator(
        task_id='get_aead_decryption_keys',
        python_callable=get_aead_decryption_keys,
        op_kwargs={'Period': '{{ dag_run.conf.get("period") }}',
                   'source_value': 'ie_act_ret_file'
                   },
        provide_context=True,
    )
    
    load_aead_key_info_table = PythonOperator(
        task_id="load_aead_key_info_table",
        python_callable=load_aead_key_info_table,
        op_kwargs={
            'period': '{{ dag_run.conf.get("period") }}',
            'source_value': 'ie_franchise_acquisition_and_retention_calculation',
            'key_name': vault_secret_key_name
        },
        provide_context=True,
        trigger_rule="none_failed"
    )
    
    #delete files after successful calculation
    delete_modified_files = BashOperator(
        task_id='delete_modified_files',
        bash_command="gsutil -m rm -r gs://{bucket}/IE_ACT_RET_CALCULATION/Reports/**"
            .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values', key='directory')}}",
                    bucket=Variable.get('ie_bucket')),
        trigger_rule="all_done"
    )
    
    
    load_batches_table = PythonOperator(
        task_id="load_batches_table",
        python_callable=load_batches_table,
        op_kwargs={
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'timestamp_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='timestamp_value')}}",
            'payment_period': '{{ dag_run.conf.get("period") }}',
            'batch_name': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='CustomBatchName')}}"
        },
        provide_context=True,
        trigger_rule="one_success"
    )
    
    # load files, batchfiles table for calculation report
    load_files_and_batchfiles_table = PythonOperator(
        task_id="load_files_and_batchfiles_table",
        python_callable=load_files_and_batchfiles_table,
        op_kwargs={
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='directory' )}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'timestamp_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='timestamp_value')}}",
            'gpg_vault_secret_key': vault_secret_key_name
        },
        provide_context=True,
        trigger_rule="all_success"
    )
   

    end = DummyOperator(
        task_id='end'
    )

    start >> get_runtime_values >> parameters_check
    parameters_check >> fetch_latest_encryption_key >> fetch_latest_encryption_pass
    fetch_latest_encryption_pass >> get_rate_file >> key_load_as_parameter
    
    key_load_as_parameter >> get_latest_batch >> trigger_pipeline
    trigger_pipeline >> report_generation
    trigger_pipeline >> get_aead_decryption_keys >> load_aead_key_info_table >> end
    
    report_generation >> delete_modified_files >> end
    report_generation >> load_files_and_batchfiles_table >> load_batches_table >> end
    