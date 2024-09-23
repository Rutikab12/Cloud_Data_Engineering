from datetime import datetime
import pandas as pd
import glob
import os
import re
import shutil
from google.cloud import bigquery
import time
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
import logging
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from CustomVaultOperator import CustomVaultOperator
import CustomVaultClass
import gnupg
import uuid

# These args will get passed on to each operator
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


# Directory value here will depict folder in GCS bucket
# directory_value = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')

def start(**kwargs):
    task_instance = kwargs['ti']
    dag_id = kwargs['dag_run'].dag_id
    dag_run_id = kwargs['dag_run'].run_id
    directory_value = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    task_instance.xcom_push(key='directory', value=directory_value)
    task_instance.xcom_push(key='timestamp', value=timestamp_value)
    task_instance.xcom_push(key='dag_id_value', value=dag_id)
    task_instance.xcom_push(key='dag_run_id_value', value=dag_run_id)

def get_runtime_values(**kwargs):
    task_instance = kwargs['ti']
    sql1 = kwargs['get_batch_id']
    sql2 = kwargs['update_batch_id']

    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_cz', schema=Variable.get('cloudsql_schema_cz'))
    directory_value = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    dag_id = kwargs['dag_run'].dag_id
    dag_run_id = kwargs['dag_run'].run_id

    pg_hook.run(sql2)
    records = pg_hook.get_records(sql=sql1)
    batch_id = str(records[0][0])
    logging.info("batch_id : " + batch_id)

    CustomBatchName = "CALC_ICT" + str(records[0][0])
    logging.info("CustomBatchName : " + CustomBatchName)

    sql3 = "select xprimarychannel,xbusinessunit,xchannel,xbatchtype,xrawdatatype from cz_lite_input_config " \
           "where workflow = 'cz_calc_ict';"
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

    task_instance.xcom_push(key='directory', value=directory_value)
    task_instance.xcom_push(key='batch_id', value=batch_id)
    task_instance.xcom_push(key='XPrimaryChannel', value=XPrimaryChannel)
    task_instance.xcom_push(key='XBusinessUnit', value=XBusinessUnit)
    task_instance.xcom_push(key='XChannel', value=XChannel)
    task_instance.xcom_push(key='XBatchType', value=XBatchType)
    task_instance.xcom_push(key='XRawDataType', value=XRawDataType)
    task_instance.xcom_push(key='CustomBatchName', value=CustomBatchName)
    task_instance.xcom_push(key='timestamp_value', value=timestamp_value)
    task_instance.xcom_push(key='directory', value=directory_value)

    task_instance.xcom_push(key='dag_id_value', value=dag_id)
    task_instance.xcom_push(key='dag_run_id_value', value=dag_run_id)


def parameters_check(**kwargs):
    task_instance = kwargs['ti']
    XCommissionPeriod = kwargs['XCommissionPeriod']
    required_params = ["XCommissionPeriod"]

    for key in kwargs:
        value = kwargs[key]
        logging.info("The KEY is : " + str(key))
        if key in required_params:
            logging.info("key: " + str(key) + " data type : " + str(type(value)))
            if len(str(value)) == 0 or (str(value) == 'None'):
                raise AirflowFailException(str(key) + " Parameter is null.")

def get_aead_decryption_keys(**kwargs):
    task_instance = kwargs['ti']
    Period = kwargs['Period']
    source_pay = kwargs['source_value']
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_cz", schema=Variable.get('cloudsql_schema_cz'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    def return_sql(source):
        sql = "select distinct \"secretkey\" from aead_key_info where source ='" + source + "';"
        return sql

    sql_to_run = return_sql(source_pay)
    logging.info("AEAD INFO SQL: " + sql_to_run)
    cursor.execute(sql_to_run)
    result = cursor.fetchone()
    decrypt_key_name = source_pay + "_aead_key"
    if result is None:
        logging.info(decrypt_key_name + " : " + 'NONE!!! setting default key')
        logging.info("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
        task_instance.xcom_push(key=decrypt_key_name, value=Variable.get('cz-default-key'))
    else:
        task_instance.xcom_push(key=decrypt_key_name, value=str(result[0]))


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

    logging.info(f'File copied from {source_blob} to {new_blob_name}')

def get_ref_file_and_decrypt(**kwargs):
    task_instance = kwargs['ti']
    directory = kwargs['directory']
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_cz", schema=Variable.get('cloudsql_schema_cz'))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'
    # Change the decrypted path
    decrypted_path = '/home/airflow/gcs/data/cz/cz_calc_ict_original/decrypted/'
    if not os.path.exists(decrypted_path):
        os.makedirs(decrypted_path)

    # SQL to fetch reffile decryption key
    def return_sql(filename):
        sql = "select \"secretKeyName\" from files where \"filePath\" like " \
              "'%" + filename + "' order by \"createdAt\" desc limit 1;"
        return sql

    file_period = kwargs['file_period']
    logging.info('file_period_key : ' + str(file_period))

    for key in kwargs:

        if 'file_type' in key:
            logging.info('file_type_key : ' + str(key))
            file_type_key = str(key)
            file_type = kwargs[key]
            client = storage.Client()
            if 'ref_file_type' in key:
                blobs = client.list_blobs(Variable.get('cz-bucket-name'), prefix='refFiles/approved')
            elif 'trans_file_type' in key:
                blobs = client.list_blobs(Variable.get('cz-bucket-name'), prefix='transFiles/approved')

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
                        # head, tail = os.path.split(file_list[i])
                        decrypt_key_name = 'DEC_KEY_' + file_type_key
                        if result is None:
                            logging.info("DECRYPTION Key Not Found !! SQL : " + sql_to_run)
                            raise AirflowFailException(decrypt_key_name + " key not found.")
                        else:
                            logging.info("File present")

                            vault_addr = Variable.get('vault_addr')
                            vault_role = Variable.get('vault_role_cz')
                            vault_secret = Variable.get('vault_path_cz')
                            vault_secret_key = str(result[0])
                            vault_secret_pass = CustomVaultClass.execute(vault_addr, vault_role, vault_secret,
                                                                         vault_secret_key)
                            head, tail = os.path.split(filename)
                            # Change the destination path below
                            destination_path = 'data/cz/cz_calc_ict_original/' + tail
                            logging.info("DESTINATION PATH :" + str(destination_path))
                            decrypted_path_final = decrypted_path + tail
                            logging.info("DECRYPTED PATH :" + str(decrypted_path_final))

                            mv_blob(Variable.get('cz-bucket-name'), filename, Variable.get('dag_bucket'),
                                    destination_path)

                            try:

                                with open('/home/airflow/gcs/' + destination_path, 'rb') as f:

                                    decryptionStatus = gpg.decrypt_file(f, output=decrypted_path_final,
                                                                        passphrase=vault_secret_pass)
                                    logging.info("STATUS OK ? " + str(decryptionStatus.ok))
                                    logging.info("STDERR: " + str(decryptionStatus.stderr))

                                    # Change the modified path below

                                    if (decryptionStatus.ok):
                                        print("File decrypted successfully")
                                    else:
                                        print("File decryption failed, Error details: " + str(decryptionStatus.status))

                                final_bucket_path = 'gs://' + Variable.get(
                                    'cz-bucket-name') + '/' + 'cz_calc_ict_modified/' + \
                                                    directory + '/' + tail
                                break

                            except Exception as e:
                                print("Exception occurred: " + str(e))

                if filename is None:
                    raise AirflowFailException("Error : File Not Found !!")
                else:
                    task_instance.xcom_push(key=file_type_key, value=final_bucket_path)


def clean_headers_and_encrypt(**kwargs):
    path_value = kwargs['path_value']
    GPG_latest_pass = kwargs['GPG_latest_pass']
    directory_value = kwargs['directory']

    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    all_files = glob.glob(path_value + '/*')
    # Change the outdir path below
    outdir = '/home/airflow/gcs/data/cz/cz_calc_ict_modified/'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    try:
        files = list(filter(lambda file: os.stat(file).st_size > 0, all_files))

        for filename in files:
            logging.info("FILENAME :" + str(filename))
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
                    df.columns = [a.upper() for a in df.columns]

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

                # Change encrypted path below
                encrypted_path = "/home/airflow/gcs/data/cz/cz_calc_ict_modified/encrypted/"
                if not os.path.exists(encrypted_path):
                    os.makedirs(encrypted_path)
                # Change path below
                with open("/home/airflow/gcs/data/cz/cz_calc_ict_modified/" + tail, 'rb') as f:
                    status = gpg.encrypt_file(f, recipients=None, output=encrypted_path + tail, passphrase=GPG_latest_pass,
                                              symmetric=True, armor=False)
                    logging.info("STATUS OK ? " + str(status.ok))
                    logging.info("STDERR: " + str(status.stderr))


            # This code needs to be tested for CSV files 10-Feb-2023
            elif filename.endswith(".csv"):  # for all files which are not .xlsx
                logging.info("CSV FILENAME :" + str(filename))
                df = pd.read_csv(filename, sep=',', encoding='latin1')
                df.rename(columns=lambda x: x.strip())
                df.columns = df.columns.str.replace('%', 'percent')
                df.columns = [re.sub('[^A-Za-z0-9_]+', '_', str(c)) for c in df.columns]
                df.columns = df.columns.str.replace('__', '_')
                df.columns = [re.sub('_$', '', str(c)) for c in df.columns]
                df.columns = [d.upper() for d in df.columns]
                df.to_csv(filename, index=False)
                head, tail = os.path.split(filename)

                # Change path below
                encrypted_path = "/home/airflow/gcs/data/cz/cz_calc_ict_modified/encrypted/"

                if not os.path.exists(encrypted_path):
                    os.makedirs(encrypted_path)

                with open(filename, 'rb') as f:
                    status = gpg.encrypt_file(f, recipients=None, output=encrypted_path + tail, passphrase=GPG_latest_pass,
                                              symmetric=True, armor=False)
                    logging.info("STATUS OK ? " + str(status.ok))
                    logging.info("STDERR: " + str(status.stderr))

        # copy encrypted files from dag bucket to frontend bucket

        client = storage.Client()
        # Change the path below
        source_directory = 'data/cz/cz_calc_ict_modified/encrypted/'
        blobs = client.list_blobs(Variable.get('dag_bucket'), prefix=source_directory)
        for blob in blobs:
            file_name = str(blob.name)
            head, tail = os.path.split(file_name)
            # Change the path below
            destination_path = 'cz_calc_ict_modified' + '/' + directory_value + '/' + tail
            mv_blob(Variable.get('dag_bucket'), file_name, Variable.get('cz-bucket-name'), destination_path)
            logging.info(str(file_name) + "has been moved successfully")

        # delete the local files
        # Change the path below +
        shutil.rmtree("/home/airflow/gcs/data/cz/cz_calc_ict_original")
        shutil.rmtree("/home/airflow/gcs/data/cz/cz_calc_ict_modified")
    except Exception as e:
        print("Exception occurred: " + str(e))
        shutil.rmtree("/home/airflow/gcs/data/cz/cz_calc_ict_original")
        shutil.rmtree("/home/airflow/gcs/data/cz/cz_calc_ict_modified")
        raise AirflowFailException("Error : Clean Headers Task Failed !!")

def load_aead_key_info_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_cz", schema=Variable.get('cloudsql_schema_cz'))
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

    insert_sql = 'insert into aead_key_info ("source", "period","secretkey") values (\'' + source_pay + '\',\'' + Period + '\',\'' + key_name + '\');'

    logging.info("INSERT AEAD INFO SQL: " + insert_sql)
    cursor.execute(insert_sql)

    conn.commit()
    conn.close()

def excel_name_generation(batch_id, unique_value, xcommissionperiod, type):
    original_filename = str(batch_id) + '_Calc_ICT_' + \
                        unique_value + '_' + xcommissionperiod + '_' + type + '.xlsx'

    uuid_file_string = uuid.uuid4().hex.upper()

    encrypted_filename = str(batch_id) + '_Calc_ICT_' + \
                         str(uuid_file_string) + '_' + xcommissionperiod + '_' + type + '.xlsx'

    return original_filename, encrypted_filename


def check_file_size(**kwargs):
    # create client
    directory_value = kwargs['directory_value']
    client = storage.Client(Variable.get('project_id'))
    prefix_directory = 'Calc_ICT/Reports/' + directory_value + '/SUMMARY'
    # get bucket
    bucket = client.get_bucket(Variable.get('cz-bucket-name'))
    all_blobs = bucket.list_blobs(prefix=prefix_directory)
    size = 0
    for blob in all_blobs:
        size += int(blob.size)

    print("Output File Size is : " + str(size))

    if size == 0:
        raise AirflowFailException("Error : Pipeline generated zero output files")

def split_data(**kwargs):
    task_instance = kwargs['ti']
    ip_file_path = kwargs['ip_file_path']
    unique_col = kwargs['unique_col']
    output_folder = kwargs['output_folder']
    batch_id = kwargs['batch_id']
    unique_col_sheet = kwargs['unique_col_sheet']
    email_id = kwargs['userid']
    xcommissionperiod = kwargs['XCommissionPeriod']
    ip_file_name = ip_file_path + '/' + kwargs['ip_file_name']
    print("IO File :- " + ip_file_name)

    df = pd.read_excel(ip_file_name, sheet_name=None)
    print(type(df))
    colList = []
    colList = list(df.keys())
    print(colList)

    data = df.get(unique_col_sheet)
    print(data)
    unique_values = data[unique_col].str.lower().unique()
    print(unique_values)
    

    for unique_value in unique_values:
        if unique_value != 'celkem':
            print(unique_value)

            original_filename, encrypted_filename = excel_name_generation(batch_id, unique_value, xcommissionperiod,output_folder)
            logging.info("original_filename: " + original_filename)
            logging.info("encrypted_filename: " + encrypted_filename)

            op_file_path = ip_file_path + '/' + str(encrypted_filename)
            logging.info("Output File path: " + op_file_path)
            # insert original_filename <-> encrypted_filename mapping in cloud sql table
            pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_cz', schema=Variable.get('cloudsql_schema_cz'))

            sql = "insert into filename_mapping(original_filename , encrypted_filename)values('" + original_filename + "','" + encrypted_filename + "' ); "
            pg_hook.run(sql)

            logging.info("Insert SQL Run :" + str(sql))
            writer = pd.ExcelWriter(op_file_path)
            for key in colList:
                print("Key :" + key)
                data = df.get(key)
                if unique_col in data.columns:
                    df_output = data[data[unique_col].astype(str).str.lower().str.contains(unique_value.lower())]
                    UserEmail = email_id
                    df_output['UserEmail'] = UserEmail
                    print(df_output)

                    if df_output.empty:
                        print("Dataframe is empty")
                        pass
                    else:
                        df_output.to_excel(writer, sheet_name=key, index=False)
            writer.save()

def encrypt_excels(**kwargs):
    directory_value = kwargs['directory']
    GPG_latest_pass = kwargs['GPG_latest_pass']

    gpg = gnupg.GPG()
    gpg.encoding = 'latin1'

    # copy files from frontend bucket to dag bucket for encryption
    client = storage.Client()
    # Change the name below
    prefix_directory = 'Calculation_output/Calc_ICT' + '/' + directory_value
    blobs = client.list_blobs(Variable.get('cz-bucket-name'), prefix=prefix_directory)
    for blob in blobs:
        filename = str(blob.name)
        head, tail = os.path.split(filename)
        # Change the destination path below
        destination_path = 'data/cz/cz_calc_ict_outputs/decrypted/' + tail
        mv_blob(Variable.get('cz-bucket-name'), filename, Variable.get('dag_bucket'), destination_path)

    # delete raw_output
    # os.remove("/home/airflow/gcs/data/cz/helpdesk_senior_m_outputs/decrypted/_SUCCESS")
    # for f in glob.glob("/home/airflow/gcs/data/cz/helpdesk_senior_m_outputs/decrypted/*-r-*"):
    #     os.remove(f)

    # encrypt the files
    # Change the encrypted path below
    encrypted_file_path = '/home/airflow/gcs/data/cz/cz_calc_ict_outputs/encrypted/'
    if not os.path.exists(encrypted_file_path):
        os.makedirs(encrypted_file_path)

    # Change the encrypted path below
    decrypted_file_path = '/home/airflow/gcs/data/cz/cz_calc_ict_outputs/decrypted/'
    all_files = glob.glob(decrypted_file_path + '/*')
    files = list(filter(lambda file: os.stat(file).st_size > 0, all_files))
    for filename in files:
        head, tail = os.path.split(filename)
        with open(filename, 'rb') as f:
            gpg.encrypt_file(f, recipients=None, output=encrypted_file_path + tail, passphrase=GPG_latest_pass,
                             symmetric=True, armor=False)

    # copy encrypted files from dag bucket to frontend bucket
    # Change the encrypted path below
    prefix_directory_return = 'data/cz/cz_calc_ict_outputs/encrypted'
    blobs_return = client.list_blobs(Variable.get('dag_bucket'), prefix=prefix_directory_return)
    for blob in blobs_return:
        filename = str(blob.name)
        head, tail = os.path.split(filename)
        destination_path = 'Calculation_output/Calc_ICT' + '/' + directory_value + '/encrypted/' + tail
        mv_blob(Variable.get('dag_bucket'), filename, Variable.get('cz-bucket-name'), destination_path)

    # Remove files from dag bucket
    #shutil.rmtree(encrypted_file_path)
    #shutil.rmtree(decrypted_file_path)

def loading_workflowOutputFiles_tables(**kwargs):
    task_instance = kwargs['ti']
    excel_path = kwargs['excel_path']
    timestamp_value = kwargs['timestamp_value']
    dag_run_id = kwargs['dag_run'].run_id
    dag_id = kwargs['dag_run'].dag_id
    batch_id = kwargs['batch_id']
    xcommissionperiod = kwargs['xcommissionperiod']
    batch_name = kwargs['batch_name']
    gpg_vault_secret_key = kwargs['gpg_vault_secret_key']

    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_cz', schema=Variable.get('cloudsql_schema_cz'))

    sql1 = "select id  from \"approvalStatus\" where lower(\"name\")= lower('New');"
    records = pg_hook.get_records(sql1)
    approval_status_id = records[0][0]
    logging.info("Approval Status :" + str(approval_status_id))

    sql2 = "select id  from \"batch_type\" where lower(\"type\")= lower('payment');"
    records = pg_hook.get_records(sql2)
    batch_type_id = records[0][0]
    logging.info("Batch Type :" + str(batch_type_id))

    # is_backup and updated_at column update condition check and updating
    sql3 = "select name from \"approvalStatus\" INNER JOIN batches ON \"approvalStatus\".id = batches.\"approvalStatusId\" " \
           "AND batches.batch_name = '" + batch_name + "' order by batches.batch_id ;"
    result = pg_hook.get_records(sql3)
    logging.info(".....SQL :" + str(sql3))
    logging.info("Result :" + str(result))

    if len(result) != 0:
        is_backup = result[0][0]
        if is_backup.lower() == "new":
            sql5 = "update batches set is_backup=True, updated_at='" + timestamp_value + "' where batch_name = '" + \
                   batch_name + "' AND batches.period='" + xcommissionperiod + "' AND \"approvalStatusId\"=7;commit;"
            pg_hook.run(sql5)
            logging.info("Update SQL Run :" + str(sql5))

    sql4 = "insert into batches(period, batch_name, batch_id, created_at, updated_at, is_active, \"approvalStatusId\", \"isApproved\", batch_type_id) " \
           "values('" + xcommissionperiod + "','" + batch_name + "'," + str(
        batch_id) + ",'" + timestamp_value + "','" + timestamp_value + "','t'," + str(
        approval_status_id) + ",'0'," + str(batch_type_id) + ");"
    pg_hook.run(sql4)

    logging.info("Insert SQL Run :" + str(sql4))

    file_name_list = []
    client = storage.Client()
    blobs = client.list_blobs(Variable.get('cz-bucket-name'), prefix=excel_path)

    for blob in blobs:
        filename = str(blob.name).split("/")[-1]
        file_name_list.append(filename)

    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_cz', schema=Variable.get('cloudsql_schema_cz')).get_conn()
    cursor = pg_hook.cursor()

    if len(file_name_list) != 0:

        for idx, file_name in enumerate(file_name_list):

            file_path = excel_path + "/" + file_name

            # inserting data into files table

            if len(file_name) != 0:

                sql1 = "select original_filename from filename_mapping where encrypted_filename =  '" + file_name + "';"
                logging.info("SQL :" + str(sql1))
                cursor.execute(sql1)
                result = cursor.fetchall()
                if len(result) != 0:
                    orginal_file_name = str(result[0][0])
                else:
                    orginal_file_name = file_name

                sql1 = "insert into files(\"fileName\", \"originalFileName\", \"filePath\",\"createdAt\",\"updatedAt\"," \
                       "\"secretKeyName\") values('" + file_name + "','" + orginal_file_name + "','" + file_path + "','" \
                       + timestamp_value + "','" + timestamp_value + "','" + gpg_vault_secret_key + "') RETURNING id;"
                logging.info("files insert data sql:- " + sql1)

                cursor.execute(sql1)
                file_id = cursor.fetchone()[0]
                pg_hook.commit()
                logging.info("id generated for files :- " + str(file_id))

                # Inserting data into batchFiles table
                sql2 = "insert into \"batchFiles\"(\"fileId\", \"createdAt\", \"batchId\") " \
                       "values (" + str(file_id) + ",'" + timestamp_value + "'," + str(batch_id) + ");"

                logging.info("batchFiles insert data sql: " + sql2)
                cursor.execute(sql2)
                pg_hook.commit()
    else:
        cursor.close()
        raise AirflowFailException("Error : output files are not generated !!")

    cursor.close()

def deleting_bigquery_staging_data(**kwargs):
    project_id = Variable.get('project_id')
    staging_dataset = Variable.get('cz_staging_dataset')

    client = bigquery.Client(
        project=project_id
    )
    table_list = ['ICT_Summary_Summarize','CA_Sum_Sink','ICT_Calc_CA_FINAL','ICT_Master_data_Sink','Pocket_Cashback_Sum','ICT_Calc_Cashback_Final',
                  'ICT_Data_Sum','ICT_Calc_OSTATNI_Final','ICT_Summary_final']

    for table in table_list:
        delete_query = """DELETE FROM `""" + project_id + """.""" + staging_dataset + """.""" + table \
                       + """` where true;"""
        logging.info("Delete Query : " + str(delete_query))
        client.query(delete_query)



with DAG(
        'cz_calc_ict',
        default_args=default_args,
        description="{\"name\":\"CZ_Calc_ICT Pipeline\",\"description\":\"**This workflow executes pipeline for Calculation Franchise_M.**\\n\\n**It takes below inputs:-**\\n1. Prepaid.xlsx file\\n2. CA.xlsx file\\n3. Volume.xlsx file\\n4. Manual_adjustment.xlsx file from Cloud Storage and generates Summary and Login Report .\\n\\n**This workflow has below tasks:**\\n1. Get parameter values \\n2. Basis parameter values fetch reference/ transaction files from Cloud Storage.\\n3. Trigger Pipeline for CZ_Calc_ICT in Cloud Data Fusion.\\n4. Generate Summary and Partners report Excel.\",\"fields\":[{\"name\":\"xcommissionperiod\",\"type\":\"date\",\"label\":\"XCommissionPeriod\",\"format\":\"yyyy_MM\"}]}",
        start_date=datetime(2022, 1, 1),
        catchup=False,
        schedule_interval=None,
        max_active_runs=1,
        tags=['cz_calculation'],
) as dag:
    # file_type_target= "'"+ '{{ dag_run.conf.get("file_type_for_target") }}' + "'"
    start = PythonOperator(
        task_id='start',
        python_callable=start,
        provide_context=True,
    )

    parameters_check = PythonOperator(
        task_id='parameters_check',
        python_callable=parameters_check,
        op_kwargs={
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}'
        }
    )

    get_runtime_values = PythonOperator(
        task_id='get_runtime_values',
        python_callable=get_runtime_values,
        provide_context=True,
        op_kwargs={
            'get_batch_id': 'select max(batch_id) from latest_batch_id_calc;',
            'update_batch_id': 'update latest_batch_id_calc set batch_id=batch_id+1 where true;commit;'
        },
        pool='batch_id_payment_cz'
    )

    fetch_latest_encryption_key = CustomVaultOperator(
        task_id='fetch_latest_encryption_key',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_cz'),
        vault_secret=Variable.get('vault_path_cz'),
        vault_secret_key="latest_key_name"
    )
    vault_secret_key_name = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_key',key='vault_value')}}"

    fetch_latest_encryption_pass = CustomVaultOperator(
        task_id='fetch_latest_encryption_pass',
        vault_addr=Variable.get('vault_addr'),
        vault_role=Variable.get('vault_role_cz'),
        vault_secret=Variable.get('vault_path_cz'),
        vault_secret_key=vault_secret_key_name
    )
    secret_pass = "{{task_instance.xcom_pull(task_ids='fetch_latest_encryption_pass',key='vault_value')}}"

    get_aead_decryption_keys = PythonOperator(
        task_id='get_aead_decryption_keys',
        python_callable=get_aead_decryption_keys,
        op_kwargs={'Period': '{{ dag_run.conf.get("xcommissionperiod") }}',
                   'source_value': 'cz_calc_ict'
                   },
        provide_context=True,
    )

    get_ref_file_and_decrypt = PythonOperator(
        task_id='get_ref_file_and_decrypt',
        python_callable=get_ref_file_and_decrypt,
        op_kwargs={
            'file_period': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'ref_file_type_for_calc_master_data_indirect_ict': Variable.get(
                'cz_file_type_for_calc_master_data_indirect_ict'),
            'ref_file_type_for_calc_mapping_file_ict': Variable.get(
                'cz_file_type_for_calc_mapping_file_ict'),
            'trans_file_type_for_calc_indirect_ict_acq_ca': Variable.get(
                'cz_file_type_for_calc_indirect_ict_acq_ca'),
            'trans_file_type_for_calc_indirect_ict_acq_summary': Variable.get(
                'cz_file_type_for_calc_indirect_ict_acq_summary'),
            'trans_file_type_for_calc_indirect_ict_acq_cashback': Variable.get(
                'cz_file_type_for_calc_indirect_ict_acq_cashback'),
            'trans_file_type_for_calc_ict_manualni_adjustmenty': Variable.get(
                'cz_file_type_for_calc_ict_manualni_adjustmenty'),

            'directory': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}"
        },
        trigger_rule="one_success"
    )

    clean_headers_and_encrypt = PythonOperator(
        task_id="clean_headers_and_encrypt",
        python_callable=clean_headers_and_encrypt,
        op_kwargs={
            'path_value': "/home/airflow/gcs/data/cz/cz_calc_ict_original/decrypted/",
            'directory': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'GPG_latest_pass': secret_pass
        },
        trigger_rule="all_success",
        provide_context=True
    )

    trigger_pipeline_ca = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline_ca",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_ca',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'indirect_ict_acq_ca': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                                "key='trans_file_type_for_calc_indirect_ict_acq_ca')}}",
            'master_data_indirect': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                                "key='ref_file_type_for_calc_master_data_indirect_ict')}}",
            'mapping_file': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                                "key='ref_file_type_for_calc_mapping_file_ict')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )

    trigger_pipeline_summary = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline_summary",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_summary',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'indirect_ict_acq_summary': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='trans_file_type_for_calc_indirect_ict_acq_summary')}}",
            'master_data_indirect': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_master_data_indirect_ict')}}",
            'mapping_file': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_mapping_file_ict')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )

    trigger_pipeline_zakalad = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline_zakalad",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_zaklad_objem_kvalita',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'indirect_ict_acq_summary': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='trans_file_type_for_calc_indirect_ict_acq_summary')}}",
            'master_data_indirect': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_master_data_indirect_ict')}}",
            'mapping_file': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_mapping_file_ict')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )
    
    trigger_pipeline_cashback = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline_cashback",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_cashback',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'indirect_ict_acq_cashback': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='trans_file_type_for_calc_indirect_ict_acq_cashback')}}",
            'master_data_indirect': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_master_data_indirect_ict')}}",
            'mapping_file': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_mapping_file_ict')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )

    trigger_pipeline_ostatni = CloudDataFusionStartPipelineOperator(
        task_id="trigger_pipeline_ostatni",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_ostatni',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'ict_manualni_adjustmenty': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='trans_file_type_for_calc_ict_manualni_adjustmenty')}}",
            'master_data_indirect': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_master_data_indirect_ict')}}",
            'mapping_file': "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',"
                            "key='ref_file_type_for_calc_mapping_file_ict')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )
    
    trigger_summary_partner_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="trigger_summary_partner_pipeline",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict_summary_partner',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="one_success"
    )
    

    trigger_summary_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="trigger_summary_pipeline",
        location=Variable.get('pl_region'),
        pipeline_name='cz_calc_ict',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'project_id': Variable.get('project_id'),
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}",
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'gcs_region': Variable.get('gcs_region'),
            'bq_region': Variable.get('bq_region'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'DEC_KEY_GPG': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=20 * 60,
        trigger_rule="all_success"
    )

    deleting_bigquery_staging_data = PythonOperator(
        task_id="deleting_bigquery_staging_data",
        python_callable=deleting_bigquery_staging_data,
        provide_context=True,
        trigger_rule="all_done"

    )

    cz_lite_calculation_macro = CloudDataFusionStartPipelineOperator(
        task_id="cz_lite_calculation_macro",
        location=Variable.get('pl_region'),
        pipeline_name='cz_lite_macro',
        instance_name=Variable.get('instance_name'),
        success_states=['COMPLETED'],
        namespace=Variable.get('cz_namespace'),
        runtime_args={
            'XPrimaryChannel': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XPrimaryChannel')}}",
            'XBusinessUnit': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XBusinessUnit')}}",
            'XChannel': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XChannel')}}",
            'XBatchType': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XBatchType')}}",
            'XRawDataType': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='XRawDataType')}}",
            'CustomBatchName': "Calc_ICT",
            'XBatchNumber': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'vault_secret_key_decs': "{{task_instance.xcom_pull(task_ids='get_aead_decryption_keys', "
                                     "key='cz_calc_ict_aead_key')}}",
            'source': "cz_calc_ict",
            'cz-bucket-name': Variable.get('cz-bucket-name'),
            'cz_lite_input_table_name': "t_cz_lite_input_ict",
            'cz_lite_intermediate_table_name': Variable.get('cz_lite_intermediate_table_name'),
            'cz_lite_output_table_name': Variable.get('cz_lite_output_table_name'),
            'system.profile.name': Variable.get('cz.system.profile.name'),
            'project_id': Variable.get('project_id'),
            'gold_dataset': Variable.get('cz_gold_dataset'),
            'staging_dataset': Variable.get('cz_staging_dataset'),
            'bq_region': Variable.get('bq_region'),
            'vault_secret_key': vault_secret_key_name,
            'vault_path': Variable.get('vault_path_cz')

        },
        gcp_conn_id=Variable.get('gcp_conn_id'),
        pipeline_timeout=25 * 60,
        trigger_rule="all_success"
    )

    load_aead_key_info_table = PythonOperator(
        task_id="load_aead_key_info_table",
        python_callable=load_aead_key_info_table,
        op_kwargs={
            'period': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'source_value': 'cz_calc_ict',
            'key_name': vault_secret_key_name
        },
        provide_context=True,
        trigger_rule="none_failed"
    )

    check_file_size = PythonOperator(
        task_id='check_file_size',
        python_callable=check_file_size,
        op_kwargs={
            'directory_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values',"
                               "key='directory')}}"
        }
    )

    summary_report = TriggerDagRunOperator(
        task_id="summary_report",
        trigger_dag_id="excel_generate_dag",
        conf={
            "raw_files_path": "gs://" + Variable.get(
                'cz-bucket-name') + "/Calc_ICT/Reports/{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            "tab_list": "SUMMARY,PARTNER,ZAKLAD_OBJEM_KVALITA,CA,CASHBACK,OSTATNI",
            "final_excel_path": "gs://" + Variable.get(
                'cz-bucket-name') + "/Calculation_output/Calc_ICT/{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            "final_excel_name": "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" +
                                "_Calc_ICT_" + "{{ dag_run.conf.get('xcommissionperiod') }}" + "_SUMMARY.xlsx"

        },
        wait_for_completion=True,
        trigger_rule="one_success"
    )

    partner_report = PythonOperator(
        task_id="partner_report",
        python_callable=split_data,
        op_kwargs={
            'ip_file_path': "gs://" + Variable.get(
                'cz-bucket-name') + "/Calculation_output/Calc_ICT/"
                                    "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'ip_file_name': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" +
                            "_Calc_ICT_" + "{{ dag_run.conf.get('xcommissionperiod') }}" + "_SUMMARY.xlsx",
            'userid': '{{ dag_run.conf.get("userEmailID") }}',
            'XCommissionPeriod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'output_folder': 'PARTNER_REPORT',
            'unique_col': 'PARTNER_NM',
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'unique_col_sheet': "SUMMARY"

        },
        provide_context=True,
        trigger_rule="one_success"
    )

    encrypt_excels = PythonOperator(
        task_id='encrypt_excels',
        python_callable=encrypt_excels,
        op_kwargs={
            'directory': "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
            'GPG_latest_pass': secret_pass
        },
        trigger_rule="all_success"
    )

    delete_original_files = BashOperator(
        task_id='delete_original_files',
        bash_command="gsutil -m rm -r gs://{bucket}/Calculation_output/Calc_ICT/{directory}/*ICT*"
                     "&& gsutil -m rm -r gs://{bucket}/Calc_ICT/Reports/**"
                     "&& gsutil -m rm -r gs://{bucket}/cz_calc_ict_modified/**"
                     "&& gsutil -m rm -r gs://{dag_bucket}/data/cz/cz_calc_ict_outputs/**"
            .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values', key='directory')}}",
                    bucket=Variable.get('cz-bucket-name'), dag_bucket=Variable.get('dag_bucket')),
        trigger_rule="one_success"
    )

    loading_workflowOutputFiles_tables = PythonOperator(
        task_id='loading_workflowOutputFiles_tables',
        python_callable=loading_workflowOutputFiles_tables,
        op_kwargs={
            'batch_id': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
            'xcommissionperiod': '{{ dag_run.conf.get("xcommissionperiod") }}',
            'batch_name': "Calculation_ICT_" + '{{ dag_run.conf.get("xcommissionperiod") }}',
            'excel_path': "Calculation_output/Calc_ICT/{directory}/encrypted"
                .format(directory="{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}"),
            'timestamp_value': "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='timestamp_value')}}",
            "gpg_vault_secret_key": vault_secret_key_name
        },
        trigger_rule='all_success',
        provide_context=True
    )

    workflow_run_details = TriggerDagRunOperator(
        task_id="cz_workflow_run_details",
        trigger_dag_id="cz_workflow_run_details",
        conf={"dag_id": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='dag_id_value')}}",
              "dag_run_id": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='dag_run_id_value')}}",
              },
        wait_for_completion=True,
        trigger_rule="all_done"
    )

    # Audit monitor code
    # These parameter will sent from web portal
    triggeredbyuser = '{{ dag_run.conf.get("triggeredByUser") }}'
    userid = '{{ dag_run.conf.get("userID") }}'
    useremailid = '{{ dag_run.conf.get("userEmailID") }}'
    userrole = '{{ dag_run.conf.get("userRole") }}'

    # Note :- For pipeline taskid below , It should be passed in sequence as they are running through DAG or triggering .

    arg_to_be_pass = {

        "trigger_pipeline_ca":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='trans_file_type_for_calc_indirect_ict_acq_ca')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_master_data_indirect_ict')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_mapping_file_ict')}}"

            },
        "trigger_pipeline_summary":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='trans_file_type_for_calc_indirect_ict_acq_summary')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_master_data_indirect_ict')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_mapping_file_ict')}}"
            },
        "trigger_pipeline_zakalad":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='trans_file_type_for_calc_indirect_ict_acq_summary')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_master_data_indirect_ict')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_mapping_file_ict')}}"

            },
        "trigger_pipeline_cashback":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='trans_file_type_for_calc_indirect_ict_acq_cashback')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_master_data_indirect_ict')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_mapping_file_ict')}}"

            },            
        "trigger_pipeline_ostatni":

            {
                "source_gcs_file_location": "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='trans_file_type_for_calc_ict_manualni_adjustmenty')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_master_data_indirect_ict')}}" + ',' +
                                            "{{task_instance.xcom_pull(task_ids='get_ref_file_and_decrypt',key='ref_file_type_for_calc_mapping_file_ict')}}"

            },
        "trigger_summary_partner_pipeline":

            {
                "source_gcs_file_location": "None"
            },
        "trigger_summary_pipeline":

            {
                "source_gcs_file_location": "None"
            },
        "cz_lite_calculation_macro":

            {
                "source_gcs_file_location": "None"
            }

    }

    final_excel_path = "gs://" + Variable.get(
        'cz-bucket-name') + "/Calculation_output/Calc_ICT/{directory}/encrypted/".format(
        directory="{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}")

    sensor_dag_triggered = TriggerDagRunOperator(
        task_id="monitoring_audit",
        trigger_dag_id="cz_audit",
        conf={"dag_id": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='dag_id_value')}}",
              "dag_run_id": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='dag_run_id_value')}}",
              "triggeredbyuser": triggeredbyuser,
              "userid": userid,
              "useremailid": useremailid,
              "userrole": userrole,
              "jobtype": "Calculation",  # Calculation, Transformation, Payments
              "tab_dict": "SUMMARY,PARTNER,ZAKLAD_OBJEM_KVALITA,CA,CASHBACK,OSTATNI",
              "pipeline_task_dict": arg_to_be_pass,
              "comission_period": '{{ dag_run.conf.get("xcommissionperiod") }}',
              "directory_value": "{{task_instance.xcom_pull(task_ids='get_runtime_values',key='directory')}}",
              "data_batch_number": "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}",
              "gpg_vault_secret_key": vault_secret_key_name,

              "final_excel_name": final_excel_path + "{{task_instance.xcom_pull(task_ids='get_runtime_values', key='batch_id')}}" +
                                  "_Calc_ICT_" + "{{ dag_run.conf.get('xcommissionperiod') }}" + "_SUMMARY.xlsx",
              },
        execution_date="{{ execution_date }}",
        trigger_rule="all_done"
    )


    end = DummyOperator(
        task_id='end'
    )

    start >> get_runtime_values >> parameters_check >>fetch_latest_encryption_key >> fetch_latest_encryption_pass \
    >> get_aead_decryption_keys >> get_ref_file_and_decrypt >> clean_headers_and_encrypt >>\
    [ trigger_pipeline_ca,trigger_pipeline_zakalad,trigger_pipeline_summary,trigger_pipeline_cashback,trigger_pipeline_ostatni ] >> trigger_summary_partner_pipeline >>\
    trigger_summary_pipeline >> check_file_size >> summary_report >> partner_report >> encrypt_excels >> delete_original_files >> [sensor_dag_triggered, end]
    trigger_summary_pipeline >> deleting_bigquery_staging_data >> end
    check_file_size >> cz_lite_calculation_macro >> load_aead_key_info_table >> [sensor_dag_triggered, end]
    encrypt_excels >> loading_workflowOutputFiles_tables >> [sensor_dag_triggered, end]
    get_ref_file_and_decrypt >> workflow_run_details