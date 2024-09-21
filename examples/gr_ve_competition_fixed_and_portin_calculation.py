from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
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

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from CustomVaultOperator import CustomVaultOperator
import CustomVaultClass
import gnupg

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

# Derive directory and timestamp value to save the output file in GCS bucket.
def get_runtime_values(**kwargs):
    task_instance = kwargs['ti']
    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_gr', schema=Variable.get('cloudsql_schema_gr'))
    directory_value = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    sql3 = "select xprimarychannel,xbusinessunit,xchannel,xbatchtype,xrawdatatype from gr_lite_input_config where workflow = 'VE_Competition_Fixed_and_Portin';"
    res = pg_hook.get_records(sql=sql3)
    XPrimaryChannel = res[0][0]
    XBusinessUnit = res[0][1]
    XChannel = res[0][2]
    XBatchType = res[0][3]
    XRawDataType = res[0][4]
    logging.info("XPrimaryChannel : " + str(res[0][0]))
    logging.info("XBusinessUnit : " + str(res[0][1]))
    logging.info("XChannel : " + str(res[0][2]))
    logging.info("XBatchType : " + str(res[0][3]))
    logging.info("XRawDataType : " + str(res[0][4]))

    sql1 = kwargs['get_batch_id']
    sql2 = kwargs['update_batch_id']
    pg_hook.run(sql2)
    records = pg_hook.get_records(sql=sql1)
    batch_id = str(records[0][0])
    logging.info("batch_id : " + batch_id)

    CustomBatchName = "Greece_VE_Competition_Fixed_and_Portin_Calc"
    logging.info("CustomBatchName : " + CustomBatchName)

    task_instance.xcom_push(key='directory', value=directory_value)
    task_instance.xcom_push(key='batch_id', value=records[0][0])
    task_instance.xcom_push(key='XPrimaryChannel', value=XPrimaryChannel)
    task_instance.xcom_push(key='XBusinessUnit', value=XBusinessUnit)
    task_instance.xcom_push(key='XChannel', value=XChannel)
    task_instance.xcom_push(key='XBatchType', value=XBatchType)
    task_instance.xcom_push(key='XRawDataType', value=XRawDataType)
    task_instance.xcom_push(key='CustomBatchName', value=CustomBatchName)
    task_instance.xcom_push(key='timestamp_value', value=timestamp_value)

# Validation of DAG parameters and DAG will fail if required parameters are not provided.
def parameters_check(**kwargs):
    task_instance = kwargs['ti']
    PERIOD = kwargs['PERIOD']
    required_params = ["PERIOD"]

    for key in kwargs:
        value = kwargs[key]
        logging.info("The KEY is : " + str(key))
        if key in required_params:
            logging.info("key: " + str(key) + " data type : " + str(type(value)))
            if len(str(value)) == 0 or (str(value) == 'None'):
                raise AirflowFailException(str(key) + " Parameter is null.")

# Function to move files in the GCS Buckets
def mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """
    Function for moving files between directories or buckets. it will use GCP's copy
    function then delete the blob from the old location.
    inputs
    -----
    bucket_name: name of bucket
    blob_name: str, name of file
        ex. 'data/some_location/file_name'
    new_bucket_name: name of bucket (can be same as original if we're just moving around directories)
    new_blob_name: str, name of file in new directory in target bucket
        ex. 'data/destination/file_name'
    """
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

# Function to get reference files from approved folder and decrypt
def get_ref_file_and_decrypt(**kwargs):
    task_instance = kwargs['ti']
    directory = kwargs['directory']
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_gr", schema=Variable.get('cloudsql_schema_gr'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    # delete the previous generated local files
    if os.path.exists("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original"):
        shutil.rmtree("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original")

    if os.path.exists("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified"):
        shutil.rmtree("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified")

    decrypted_path = '/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original/decrypted/'
    if not os.path.exists(decrypted_path):
        os.makedirs(decrypted_path)

    # SQL to fetch reffile decryption key
    def return_sql(filename):
        sql = "select \"secretKeyName\" from files where \"filePath\" like " \
              "'%" + filename + "' order by \"createdAt\" desc limit 1;"
        return sql

    for key in kwargs:

        if 'file_type' in key:
            logging.info('file_type_key : ' + str(key))
            file_type_key = str(key)
            file_type = kwargs[key]

        if 'file_period' in key:
            logging.info('file_period_key : ' + str(key))
            file_period = kwargs[key]

            client = storage.Client()
            blobs = client.list_blobs(Variable.get('gr-bucket-frontend'), prefix='refFiles/approved')
            file_type = file_type.replace(' ', '_')
            reffile_to_find = file_type + '_' + file_period
            filename = None

            if reffile_to_find == "_" + file_period or reffile_to_find == file_type + "_" or reffile_to_find == "_":
                raise AirflowFailException("Error : Please Enter file type or file period for search !!")
            else:
                for blob in blobs:
                    tmp_filename = str(blob.name)

                    if tmp_filename.lower().find(reffile_to_find.lower()) != -1:
                        filename = tmp_filename
                        task_instance = kwargs['ti']

                        sql_to_run = return_sql(filename)
                        logging.info("GPG INFO SQL : " + filename + " : " + sql_to_run)
                        cursor.execute(sql_to_run)
                        result = cursor.fetchone()
                        decrypt_key_name = 'DEC_KEY_' + file_type_key
                        if result is None:
                            print("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
                            raise AirflowFailException(decrypt_key_name + " key not found.")
                        else:

                            vault_addr = Variable.get('vault_addr')
                            vault_role = Variable.get('vault_role_gr')
                            vault_secret = Variable.get('vault_path_gr')
                            vault_secret_key = str(result[0])
                            vault_secret_pass = CustomVaultClass.execute(vault_addr, vault_role, vault_secret,
                                                                         vault_secret_key)

                            head, tail = os.path.split(filename)
                            destination_path = 'data/gr/ve_competition_fixed_and_portin_original/' + tail

                            decrypted_path_final = decrypted_path + tail

                            mv_blob(Variable.get('gr-bucket-frontend'), filename, Variable.get('dag_bucket'),
                                    destination_path)

                            with open('/home/airflow/gcs/' + destination_path, 'rb') as f:
                                status = gpg.decrypt_file(f, output=decrypted_path_final, passphrase=vault_secret_pass)
                                print("STATUS OK ? " + str(status.ok))
                                print("STDERR: " + str(status.stderr))

                            final_bucket_path = 'gs://' + Variable.get(
                                'gr-bucket-frontend') + '/' + 've_competition_fixed_and_portin_modified/' + \
                                                directory + '/' + tail
                            break

                if filename is None:
                    # delete the previous generated local files
                    print("Deleting the previous generated local files")
                    if os.path.exists("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original"):
                        shutil.rmtree("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original")

                    raise AirflowFailException("Error : File Not Found : " + reffile_to_find)
                else:
                    print("filename :" + filename)
                    print("Ref file name :" + reffile_to_find)
                    task_instance.xcom_push(key=file_type_key, value=final_bucket_path)

# Clean headers and reload the files and encrypt
def clean_headers_and_encrypt(**kwargs):
    path_value = kwargs['path_value']
    GPG_latest_pass = kwargs['GPG_latest_pass']
    directory_value = kwargs['directory']

    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    all_files = glob.glob(path_value + '/*')

    outdir = '/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified/'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    files = list(filter(lambda file: os.stat(file).st_size > 0, all_files))
    try:
        for filename in files:
            if filename.endswith(".xlsx"):
                os.rename(filename, filename.replace(' ', '_'))
                filename = filename.replace(' ', '_')
                tabs = pd.ExcelFile(filename).sheet_names
                head, tail = os.path.split(filename)
                logging.info("actual_filename: " + str(tail))
                writer = pd.ExcelWriter(outdir + tail, engine='xlsxwriter')
                for i in tabs:
                    df = pd.read_excel(filename, sheet_name=i, engine='openpyxl')
                    df.rename(columns=lambda x: x.strip())
                    df.columns = df.columns.str.replace('%', 'percent')
                    df.columns = df.columns.str.replace('\'', '')
                    df.columns = [re.sub('[^A-Za-z0-9_]+', '_', str(c)) for c in df.columns]
                    df.columns = df.columns.str.replace('__', '_')
                    df.columns = [re.sub('_$', '', str(c)) for c in df.columns]

                    df.to_excel(writer, sheet_name=i, index=False)
                    worksheet = writer.sheets[i]
                    for idx, col in enumerate(df):  # loop through all columns
                        series = df[col]
                        max_len = max((
                            series.astype(str).map(len).max(),  # len of largest item
                            len(str(series.name))  # len of column name/header
                        )) + 1  # adding a little extra space
                        worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()
                # logging.info("for file: " + filename + " columns are : " + df.columns)

                encrypted_path = "/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified/encrypted/"
                if not os.path.exists(encrypted_path):
                    os.makedirs(encrypted_path)

                with open("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified/" + tail, 'rb') as f:
                    gpg.encrypt_file(f, recipients=None, output=encrypted_path + tail, passphrase=GPG_latest_pass,
                                     symmetric=True, armor=False)

            # This code needs to be tested for CSV files 10-Feb-2023
            elif filename.endswith(".csv"):  # for all files which are not .xlsx
                df = pd.read_csv(filename, sep=',', encoding='latin1')
                df.rename(columns=lambda x: x.strip())
                df.columns = df.columns.str.replace('%', 'percent')
                df.columns = [re.sub('[^A-Za-z0-9_]+', '_', str(c)) for c in df.columns]
                df.columns = df.columns.str.replace('__', '_')
                df.columns = [re.sub('_$', '', str(c)) for c in df.columns]
                df.to_csv(filename, index=False)
                head, tail = os.path.split(filename)

                encrypted_path = "/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified/encrypted/"
                if not os.path.exists(encrypted_path):
                    os.makedirs(encrypted_path)

                with open(filename, 'rb') as f:
                    gpg.encrypt_file(f, recipients=None, output=encrypted_path + tail, passphrase=GPG_latest_pass,
                                     symmetric=True, armor=False)
    except:
        # delete the previous generated local files
        print("Deleting the previous generated local files")
        if os.path.exists("/home/airflow/gcs/data/gr/retention_commission_original"):
            shutil.rmtree("/home/airflow/gcs/data/gr/retention_commission_original")

        if os.path.exists("/home/airflow/gcs/data/gr/retention_commission_modified"):
            shutil.rmtree("/home/airflow/gcs/data/gr/retention_commission_modified")

        raise AirflowFailException("Error: Failed to clean headers for filename:" + filename)

    # copy encrypted files from dag bucket to frontend bucket
    client = storage.Client()
    source_directory = 'data/gr/ve_competition_fixed_and_portin_modified/encrypted/'
    blobs = client.list_blobs(Variable.get('dag_bucket'), prefix=source_directory)
    for blob in blobs:
        file_name = str(blob.name)
        print(file_name)
        head, tail = os.path.split(file_name)
        destination_path = 've_competition_fixed_and_portin_modified' + '/' + directory_value + '/' + tail
        mv_blob(Variable.get('dag_bucket'), file_name, Variable.get('gr-bucket-frontend'), destination_path)

    # delete the local files
    if os.path.exists("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original"):
        shutil.rmtree("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original")

    if os.path.exists("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified"):
        shutil.rmtree("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_modified")

def get_aead_decryption_keys(**kwargs):
    task_instance = kwargs['ti']
    source_pay = kwargs['source_value']

    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_gr", schema=Variable.get('cloudsql_schema_gr'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    def return_sql(source):
        sql = "select distinct \"secretkey\" from aead_key_info where source ='" + source + "';"
        return sql

    sql_to_run=return_sql(source_pay)
    logging.info("AEAD INFO SQL: " + sql_to_run)
    cursor.execute(sql_to_run)
    result = cursor.fetchone()
    decrypt_key_name = source_pay + "_aead_key"
    if result is None:
        logging.info(decrypt_key_name + " : " + 'NONE!!! setting default key')
        logging.info("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
        task_instance.xcom_push(key=decrypt_key_name, value='None')
    else:
        task_instance.xcom_push(key=decrypt_key_name, value=str(result[0]))

def load_aead_key_info_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_gr", schema=Variable.get('cloudsql_schema_gr'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    Period = kwargs['period']
    key_name = kwargs['key_name']
    source_pay = kwargs['source_value']

    def return_delete_sql(source):
        sql = "delete from aead_key_info where \"source\"='" + source + "';"
        return sql


    del_sql = return_delete_sql(source_pay)
    logging.info("DELETE AEAD INFO SQL: " + del_sql)
    postgres_hook.run(del_sql)

    insert_sql = 'insert into aead_key_info ("source","secretkey") values (\'' + source_pay + '\',\'' + key_name + '\');'

    logging.info("INSERT AEAD INFO SQL: " + insert_sql)
    cursor.execute(insert_sql)

    conn.commit()
    conn.close()

def get_xcalcdetail_decryption_keys(**kwargs):
    task_instance = kwargs['ti']
    source = kwargs['source']
    source = source.split(",")
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_gr", schema=Variable.get('cloudsql_schema_gr'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    def return_sql(source):
        sql = "select distinct \"secretkey\" from xcal_key_info where source ='" + source + "';"
        return sql

    for src in source:
        logging.info("src : " + src)
        sql_to_run = return_sql(src)
        logging.info("XCALC INFO SQL: " + sql_to_run)
        cursor.execute(sql_to_run)
        result = cursor.fetchone()
        decrypt_key_name = src + "_xcalcdetails_key"
        logging.info("decrypt_key_name : " + decrypt_key_name)
        if result is None:
            logging.info(decrypt_key_name + " : " + 'NONE!!! setting default key')
            logging.info("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
            task_instance.xcom_push(key=decrypt_key_name, value='None')
        else:
            task_instance.xcom_push(key=decrypt_key_name, value=str(result[0]))

# Function to generate excel report per payee
def payee_report_excel_generate(**kwargs):
    input_excel_path = kwargs['input_excel_path']
    final_excel_path = kwargs['final_excel_path']
    required_columns = kwargs['required_columns']
    group_by_column = kwargs['group_by_column']
    batch_id = kwargs['batch_id']
    file_period = kwargs['FILE_PERIOD']
    xl = pd.ExcelFile(input_excel_path)
    logging.info(xl.sheet_names)
    excel_sheet_list = xl.sheet_names

    if 'VE_PORT' in excel_sheet_list:

        df = pd.read_excel(input_excel_path, sheet_name='VE_PORT')
        df = df[required_columns]

        unique_values = df[group_by_column].unique()
        print(unique_values)

        for unique_value in unique_values:
            print(unique_value)
            op_file_path = final_excel_path + "/ve_competition_fixed_and_portin_" + unique_value + "_" + file_period + "_" + batch_id + ".xlsx"
            print(op_file_path)
            unique_value_data = df.query("{} == '".format(group_by_column) + unique_value + "'")

            with pd.ExcelWriter(op_file_path, engine='xlsxwriter') as writer:
                unique_value_data.to_excel(writer,index=False)
            writer.save()

# Function to encrypt excels
def encrypt_excels(**kwargs):
    directory_value = kwargs['directory_value']
    GPG_latest_pass = kwargs['GPG_latest_pass']

    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    # copy files from frontend bucket to dag bucket for encryption
    client = storage.Client()
    prefix_directory = 'Calculation_output/gr_ve_competition_fixed_and_portin_calculation' + '/' + directory_value
    blobs = client.list_blobs(Variable.get('gr-bucket-frontend'), prefix=prefix_directory)
    for blob in blobs:
        filename = str(blob.name)
        head, tail = os.path.split(filename)
        destination_path = 'data/gr/ve_competition_fixed_and_portin_outputs/decrypted/' + tail
        mv_blob(Variable.get('gr-bucket-frontend'), filename, Variable.get('dag_bucket'), destination_path)

    # delete raw_output
    os.remove("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_outputs/decrypted/_SUCCESS")
    for f in glob.glob("/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_outputs/decrypted/*-r-*"):
        os.remove(f)

    # encrypt the files
    encrypted_file_path = '/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_outputs/encrypted/'
    if not os.path.exists(encrypted_file_path):
        os.makedirs(encrypted_file_path)

    decrypted_file_path = '/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_outputs/decrypted/'
    all_files = glob.glob(decrypted_file_path + '/*')
    files = list(filter(lambda file: os.stat(file).st_size > 0, all_files))
    for filename in files:
        head, tail = os.path.split(filename)
        with open(filename, 'rb') as f:
            gpg.encrypt_file(f, recipients=None, output=encrypted_file_path + tail, passphrase=GPG_latest_pass,
                             symmetric=True, armor=False)

    # copy encrypted files from dag bucket to frontend bucket
    prefix_directory_return = 'data/gr/ve_competition_fixed_and_portin_outputs/encrypted'
    blobs_return = client.list_blobs(Variable.get('dag_bucket'), prefix=prefix_directory_return)
    for blob in blobs_return:
        filename = str(blob.name)
        head, tail = os.path.split(filename)
        destination_path = 'Calculation_output/gr_ve_competition_fixed_and_portin_calculation' + '/' + directory_value + '/encrypted/' + tail
        mv_blob(Variable.get('dag_bucket'), filename, Variable.get('gr-bucket-frontend'), destination_path)

    # Remove files from dag bucket
    shutil.rmtree(encrypted_file_path)
    shutil.rmtree(decrypted_file_path)


# Load Cloud SQL Files and Workflow Output files tables to save key details
def loading_workflowOutputFiles_tables(**kwargs):
    task_instance = kwargs['ti']
    excel_path = kwargs['excel_path']
    timestamp_value = kwargs['timestamp_value']
    dag_run_id = kwargs['dag_run'].run_id
    dag_id = kwargs['dag_run'].dag_id
    gpg_vault_secret_key = kwargs['gpg_vault_secret_key']

    file_name_list = []
    client = storage.Client()
    blobs = client.list_blobs(Variable.get('gr-bucket-frontend'), prefix=excel_path)

    for blob in blobs:
        filename = str(blob.name).split("/")[-1]
        if len(filename) != 0:
            file_name_list.append(filename)

    pg_conn = PostgresHook(postgres_conn_id='cloudsql_conn_gr', schema=Variable.get('cloudsql_schema_gr')).get_conn()
    cursor = pg_conn.cursor()

    if len(file_name_list) != 0:

        sql3 = "delete from files where id in (select distinct \"fileId\" from \"workflowOutputFiles\" where" \
               " \"dagId\" = '" + dag_id + "' and \"dagRunId\" != '" + dag_run_id + "' );"
        logging.info("workflowOutputFiles delete data sql for dag_id :- " + sql3)
        cursor.execute(sql3)
        pg_conn.commit()

        for idx, file_name in enumerate(file_name_list):
            file_path = excel_path + "/" + file_name

            # inserting data into files table

            sql1 = "insert into files(\"fileName\", \"originalFileName\", \"filePath\",\"createdAt\",\"updatedAt\"," \
                   "\"secretKeyName\") values('" + file_name + "','" + file_name + "','" + file_path + "','" \
                   + timestamp_value + "','" + timestamp_value + "','" + gpg_vault_secret_key + "') RETURNING id;"
            logging.info("files insert data sql:- " + sql1)

            cursor.execute(sql1)
            file_id = cursor.fetchone()[0]
            pg_conn.commit()
            logging.info("id generated for files :- " + str(file_id))

            # inserting data into workflowOutputFiles table
            sql2 = "insert into \"workflowOutputFiles\"(\"dagId\", \"dagRunId\", \"fileId\") " \
                   "values ('" + dag_id + "','" + dag_run_id + "'," + str(file_id) + ");"

            logging.info("workflowOutputFiles insert data sql :- " + sql2)
            cursor.execute(sql2)
            pg_conn.commit()
    else:
        cursor.close()
        raise AirflowFailException("Error : output files are not generated !!")

    cursor.close()

def load_xcal_key_info_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_gr", schema=Variable.get('cloudsql_schema_gr'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    Period = kwargs['period']
    key_name = kwargs['key_name']
    source_pay = kwargs['source_value']

    def return_delete_sql(source):
        sql = "delete from xcal_key_info where \"source\"='" + source + "';"
        return sql

    del_sql = return_delete_sql(source_pay)
    logging.info("DELETE AEAD INFO SQL: " + del_sql)
    postgres_hook.run(del_sql)

    insert_sql = 'insert into xcal_key_info ("secretkey","source") values (\'' + key_name + '\',\'' + source_pay + '\');'

    logging.info("INSERT AEAD INFO SQL: " + insert_sql)
    cursor.execute(insert_sql)

    conn.commit()
    conn.close()

def start(**kwargs):
    task_instance = kwargs['ti']
    dag_id = kwargs['dag_run'].dag_id
    dag_run_id = kwargs['dag_run'].run_id
    task_instance.xcom_push(key='dag_id_value', value=dag_id)
    task_instance.xcom_push(key='dag_run_id_value', value=dag_run_id)

with DAG(
        'gr_ve_competition_fixed_and_portin_calculation',
        default_args=default_args,
        description="{\"name\":\"VE Competition fixed and Portin Calculation Pipeline\",\"description\":\"**This workflow executes pipeline for VE Competition fixed and Portin Calculation.**\\n**It takes below inputs:-**\\n1. Read the raw data from BigQuery\\n2. Reference files uploaded from Portal and generates XCalc table and various Reports as output.\\n\\n**This workflow has below tasks:-**\\n1. Get parameter values\\n2. Basis parameter values fetch reference/ transaction files from Cloud Storage.\\n3. Trigger Pipeline for VE Competition fixed and Portin Calculation in Cloud Data Fusion.\\n4. Generate Reports on Cloud Storage in excel format.\",\"fields\":[{\"name\":\"period\",\"type\":\"date\",\"label\":\"Period\",\"format\":\"yyyy_MM\"}]}",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        schedule_interval=None,
        max_active_runs=1,
        tags=['gr','gr_calculation'],
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=start,
        provide_context=True,
    )

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

    # Find latest approved reference file using reference file type and reference file period.
    get_ref_file_and_decrypt = PythonOperator(
        task_id='get_ref_file_and_decrypt',
        python_callable=get_ref_file_and_decrypt,
        op_kwargs={
            'file_type_for_fixed_target': Variable.get('gr_file_type_for_fixed_target'),
            'file_period_for_fixed_target': '{{ dag_run.conf.get("period") }}',
            'directory': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}"
        },
        trigger_rule="one_success"
    )
    fetch_latest_encryption_key = CustomVaultOperator(
        task_id='fetch_latest_encryption_key',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_gr'),
        vault_secret=Variable.get('vault_path_gr'),
        vault_secret_key="latest_key_name"
    )
    vault_secret_key_name = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_key',key='vault_value')}}"

    fetch_latest_encryption_pass = CustomVaultOperator(
        task_id='fetch_latest_encryption_pass',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_gr'),
        vault_secret=Variable.get('vault_path_gr'),
        vault_secret_key=vault_secret_key_name
    )
    secret_pass = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_pass',key='vault_value')}}"

    get_xcalcdetail_decryption_keys = PythonOperator(
        task_id='get_xcalcdetail_decryption_keys',
        python_callable=get_xcalcdetail_decryption_keys,
        op_kwargs={
                   'source': 'gr_ve_competition_fixed_and_portin_calculation'
                   },
        provide_context=True,
    )

    clean_headers_and_encrypt = PythonOperator(
        task_id="clean_headers_and_encrypt",
        python_callable=clean_headers_and_encrypt,
        op_kwargs={
            'path_value': "/home/airflow/gcs/data/gr/ve_competition_fixed_and_portin_original/decrypted/",
            'directory': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'GPG_latest_pass': secret_pass
        },
        trigger_rule="all_success",
        provide_context=True
    )


    # Trigger Data Fusion Calculation Pipeline which will save calculation output in BigQuery table
    # and generate the excel files in gcs with calculation data.
    trigger_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline",
        location=Variable.get('pl_region'),
        pipeline_name='gr_ve_competition_fixed_and_portin_calculation',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('gr_namespace'),
        runtime_args={
            'FIXED_TARGET_FILE': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='file_type_for_fixed_target')}}",

            'bucket-name': Variable.get('gr-bucket-frontend'),
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'source_max_batch_id': "{{task_instance.xcom_pull(task_ids='fetch_source_max_batch_id',key='source_max_batch_id')}}",
            'current_run_batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='batch_id')}}",
            'Commission_Period' : FILE_PERIOD,

            'project_id': Variable.get('project_id'),
            'bq_region': Variable.get('bq_region'),
            'gcs_region': Variable.get('gcs_region'),
            'staging_dataset': Variable.get('gr_staging_dataset'),
            'gold_dataset': Variable.get('gr_gold_dataset'),
            'system.profile.name': Variable.get('gr.system.profile.name'),

            'vault_path': Variable.get('vault_path_gr'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'DEC_KEY_XCALCDETAIL_Fix_Con': "{{task_instance.xcom_pull(task_ids='get_xcalcdetail_decryption_keys', key='gr_ve_competition_fixed_and_portin_calculation_xcalcdetails_key')}}",
            'source': "gr_ve_competition_fixed_and_portin_calculation"
        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="all_success"
    )

    get_aead_decryption_keys = PythonOperator(
        task_id='get_aead_decryption_keys',
        python_callable=get_aead_decryption_keys,
        op_kwargs={
            'source_value': 'gr_ve_competition_fixed_and_portin_calculation'
        },
        provide_context=True,
    )

    gr_lite_calculation_macro = CloudDataFusionStartPipelineOperator(
        task_id="gr_lite_calculation_macro",
        location=Variable.get('pl_region'),
        pipeline_name='gr_lite_macro',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('gr_namespace'),
        runtime_args={
            'XPrimaryChannel': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XPrimaryChannel')}}",
            'XBusinessUnit': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XBusinessUnit')}}",
            'XChannel': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XChannel')}}",
            'XBatchType': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XBatchType')}}",
            'XRawDataType': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XRawDataType')}}",
            'CustomBatchName': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='CustomBatchName')}}",
            'XBatchNumber': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'bucket-name': Variable.get('gr-bucket-frontend'),
            'source': "gr_ve_competition_fixed_and_portin_calculation",
            'gr_lite_input_table_name': "gr_lite_input_ve_competition_fixed_and_portin",
            'system.profile.name': Variable.get('gr.system.profile.name'),
            'project_id': Variable.get('project_id'),
            'gold_dataset': Variable.get('gr_gold_dataset'),
            'staging_dataset': Variable.get('gr_staging_dataset'),
            'bq_region': Variable.get('bq_region'),

            'vault_secret_key': vault_secret_key_name,
            'vault_secret_key_decs': "{{task_instance.xcom_pull(task_ids='get_aead_decryption_keys', key='gr_ve_competition_fixed_and_portin_calculation_aead_key')}}",
            'vault_path': Variable.get('vault_path_gr')
        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=25 * 60,
        trigger_rule="one_success"
    )

    load_aead_key_info_table = PythonOperator(
        task_id="load_aead_key_info_table",
        python_callable=load_aead_key_info_table,
        op_kwargs={
            'period': '{{ dag_run.conf.get("period") }}',
            'source_value': 'gr_ve_competition_fixed_and_portin_calculation',
            'key_name': vault_secret_key_name
        },
        provide_context=True,
        trigger_rule="none_failed"
    )

    # Creating an excel file with calculation result data generated by data fusion pipeline.
    all_records_excel_generate = TriggerDagRunOperator(
        task_id="all_records_excel_generate",
        trigger_dag_id="excel_generate_dag",
        conf={"raw_files_path": "gs://" + Variable.get('gr-bucket-frontend')
                                + "/Calculation_output/gr_ve_competition_fixed_and_portin_calculation/"
                                  "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}/raw_output/VE_PORT",
              "tab_list": "VE_PORT",
              "final_excel_path": "gs://" + Variable.get('gr-bucket-frontend')
                                  + "/Calculation_output/gr_ve_competition_fixed_and_portin_calculation/"
                                    "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}/excel_report_output",
              "final_excel_name": "VE_Competition_Fixed_and_Portin_Detailed_Report_" + FILE_PERIOD + "_" +
                                  "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" + ".xlsx"
              },
        trigger_rule='all_success',
        wait_for_completion=True
    )

    # Creating excel file per GROUP_CODE
    payee_report_excel_generate = PythonOperator(
        task_id='payee_report_excel_generate',
        python_callable=payee_report_excel_generate,
        op_kwargs={
            "input_excel_path": "gs://" + Variable.get('gr-bucket-frontend')
                                + "/Calculation_output/gr_ve_competition_fixed_and_portin_calculation/"
                                  "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}/excel_report_output/"
                                + "VE_Competition_Fixed_and_Portin_Detailed_Report_" + FILE_PERIOD + "_" +
                                "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" + ".xlsx",
            "final_excel_path": "gs://" + Variable.get('gr-bucket-frontend')
                                + "/Calculation_output/gr_ve_competition_fixed_and_portin_calculation/"
                                  "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}/payee_report_output",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'group_by_column': "GROUP_CODE",
            "required_columns": ["COMMISSION_PERIOD","GROUP_CODE","GROUP_NAME","DLR_LEVEL","DEALER_NAME","POS_CODE","DEAL_NUMBER","CHANNEL","SUBCHANNEL","CODE_TYPE","ORDER_TYPE","ORDER_NUM","PRIMARY_CLI","ORDER_DATE","LEAD_TYPE","ITEM_CATEGORY","PRICEPLAN","TECHNOLOGY","SPEED","PRODUCTIVITY","TARGET","ACHIEVEMENT","COMPETITION_BONUS"],
            'FILE_PERIOD': FILE_PERIOD
        },
        trigger_rule="one_success"
    )

    encrypt_excels = PythonOperator(
        task_id='encrypt_excels',
        python_callable=encrypt_excels,
        op_kwargs={
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'GPG_latest_pass': secret_pass
        },
        trigger_rule="all_success"
    )

    delete_raw_files = BashOperator(
        task_id='delete_raw_files',
        bash_command="gsutil rm -r gs://{bucket}/Calculation_output/gr_ve_competition_fixed_and_portin_calculation/{directory}/*_output*"
            .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values', key='directory')}}",
                    bucket=Variable.get('gr-bucket-frontend')),
        trigger_rule="all_done"
    )

    delete_modified_files = BashOperator(
        task_id='delete_modified_files',
        bash_command="gsutil rm -r gs://{bucket}/ve_competition_fixed_and_portin_modified"
            .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values', key='directory')}}",
                    bucket=Variable.get('gr-bucket-frontend')),
        trigger_rule="all_done"
    )

    loading_workflowOutputFiles_tables = PythonOperator(
        task_id='loading_workflowOutputFiles_tables',
        python_callable=loading_workflowOutputFiles_tables,
        op_kwargs={
            'excel_path': "Calculation_output/gr_gr_ve_competition_fixed_and_portin_calculation/{directory}/encrypted"
                .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}"),
            'timestamp_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='timestamp_value')}}",
            "gpg_vault_secret_key": vault_secret_key_name
        },
        trigger_rule='all_success',
        provide_context=True
    )

    gr_lite_load_xcalcdetails = CloudDataFusionStartPipelineOperator(
        task_id="gr_lite_load_xcalcdetails",
        location=Variable.get('pl_region'),
        pipeline_name='gr_lite_load_xcalcdetails',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('gr_namespace'),
        runtime_args={
            'source': "gr_ve_competition_fixed_and_portin_calculation",
            'xcommtype': "VE Competition Fixed (New & Portin)",
            'system.profile.name': Variable.get('gr.system.profile.name'),
            'project_id': Variable.get('project_id'),
            'gold_dataset': Variable.get('gr_gold_dataset'),
            'staging_dataset': Variable.get('gr_staging_dataset'),
            'bq_region': Variable.get('bq_region'),
            'vault_secret_key_decs': "{{task_instance.xcom_pull(task_ids='get_xcalcdetail_decryption_keys', key='gr_ve_competition_fixed_and_portin_calculation_xcalcdetails_key')}}",
            'vault_secret_key': vault_secret_key_name,
            'DEC_KEY_GPG': vault_secret_key_name,
            'bucket-name': Variable.get('gr-bucket-frontend'),
            'vault_path': Variable.get('vault_path_gr')
        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=25 * 60,
        trigger_rule="one_success"
    )

    load_xcal_key_info_table = PythonOperator(
        task_id="load_xcal_key_info_table",
        python_callable=load_xcal_key_info_table,
        op_kwargs={
            'period': '{{ dag_run.conf.get("period") }}',
            'source_value': 'gr_ve_competition_fixed_and_portin_calculation',
            'key_name': vault_secret_key_name
        },
        provide_context=True,
        trigger_rule="none_failed"
    )

    # Audit monitor code

    # These parameter will sent from web portal
    triggeredbyuser = '{{ dag_run.conf.get("triggeredByUser") }}'
    userid = '{{ dag_run.conf.get("userID") }}'
    useremailid = '{{ dag_run.conf.get("userEmailID") }}'
    userrole = '{{ dag_run.conf.get("userRole") }}'

    gcs_path = "gs://" + Variable.get('gr-bucket-frontend') + "/"

    # Note :- For pipeline taskid below , It should be passed in sequence as they are running through DAG or triggering .

    arg_to_be_pass = {

        "trigger_pipeline":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='file_type_for_fixed_target')}}"
            },
        "gr_lite_calculation_macro":
            {
                "source_gcs_file_location": "None"

            },
        "gr_lite_load_xcalcdetails":
            {
                "source_gcs_file_location": "None"

            }

    }

    sensor_dag_triggered = TriggerDagRunOperator(
        task_id="monitoring_audit",
        trigger_dag_id="gr_audit",
        conf={"dag_id": "{{task_instance.xcom_pull(task_ids='start',key='dag_id_value')}}",
              "dag_run_id": "{{task_instance.xcom_pull(task_ids='start',key='dag_run_id_value')}}",
              "triggeredbyuser": triggeredbyuser,
              "userid": userid,
              "useremailid": useremailid,
              "userrole": userrole,
              "jobtype": "Calculation",  # Calculation, Validation, Payments
              "pipeline_task_dict": arg_to_be_pass,
              "payment_period": '{{ dag_run.conf.get("period") }}',
              "directory_value": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
              "data_batch_number": "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}"
              },
        execution_date="{{ execution_date }}",
        trigger_rule="all_done"
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> get_runtime_values >> parameters_check
    parameters_check >> fetch_latest_encryption_key >> fetch_latest_encryption_pass >> clean_headers_and_encrypt
    parameters_check >> get_ref_file_and_decrypt >> clean_headers_and_encrypt
    clean_headers_and_encrypt >> get_xcalcdetail_decryption_keys >> trigger_pipeline
    trigger_pipeline >> get_aead_decryption_keys >> gr_lite_calculation_macro >> load_aead_key_info_table >> end
    trigger_pipeline >> all_records_excel_generate >> payee_report_excel_generate >> encrypt_excels
    encrypt_excels >> delete_modified_files >> end
    trigger_pipeline >> gr_lite_load_xcalcdetails >> load_xcal_key_info_table >> end
    encrypt_excels >> delete_raw_files >> end
    encrypt_excels >> loading_workflowOutputFiles_tables >> end
    loading_workflowOutputFiles_tables >> sensor_dag_triggered
    load_aead_key_info_table >> sensor_dag_triggered
    load_xcal_key_info_table >> sensor_dag_triggered