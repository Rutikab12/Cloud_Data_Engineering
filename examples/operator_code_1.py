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


class ieactretingestionoperator(BaseOperator):

    template_fields = (
        'period','batch_id',
        'latest_key_name','dag_name_id','run_id')

    def __init__(self, period: str, batch_id: str, latest_key_name: str,dag_name_id: str,run_id: str,**kwargs) -> None:
        super().__init__(**kwargs)
        self.period = period
        self.batch_id = batch_id
        self.latest_key_name = latest_key_name
        self.dag_name_id = dag_name_id
        self.run_id = run_id

    def execute(self,context):
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

        # SQL to fetch reffile decryption key
        def return_sql(filename):
            sql = "select \"secretKeyName\" from files where \"filePath\" like " \
                  "'%" + filename + "' order by \"createdAt\" desc limit 1;"
            return sql

        def get_file_from_bucket(file_type, file_period, data_folder, type_flag):
            postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('ie_cloudsql_schema'))
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            gpg = gnupg.GPG()
            gpg.encoding = 'utf-8'
            client = storage.Client()
            blobs = client.list_blobs(Variable.get('ie_bucket'), prefix=type_flag + '/approved')
            file_type = file_type.replace(' ', '_')
            reffile_to_find = file_type + '_' + file_period
            filename = None

            found_flag = False

            if reffile_to_find == "_" + file_period or reffile_to_find == file_type + "_" or reffile_to_find == "_":
                raise AirflowFailException("Error : Please Enter file type or file period for search !!")
            else:
                for blob in blobs:
                    tmp_filename = str(blob.name)

                    # print("tmp_filename:" + tmp_filename)
                    if tmp_filename.lower().find(reffile_to_find.lower()) != -1:
                        filename = tmp_filename

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
                            destination_path = 'data/ie/' + data_folder +'/'+ tail

                            decrypted_path = '/home/airflow/gcs/data/ie/' + data_folder + '/dec/'
                            if not os.path.exists(decrypted_path):
                                os.makedirs(decrypted_path)

                            decrypted_path_final = decrypted_path + file_type + '.xlsx'

                            mv_blob(Variable.get('ie_bucket'), filename, Variable.get('ie_dag_bucket'),
                                    destination_path)

                            with open('/home/airflow/gcs/' + destination_path, 'rb') as f:
                                status = gpg.decrypt_file(f, output=decrypted_path_final, passphrase=vault_secret_pass)
                                print("STATUS OK ? " + str(status.ok))
                                print("STDERR: " + str(status.stderr))

                            logging.info(
                                type_flag + ' found for file_type: ' + file_type + ' and for file_period: ' + file_period)

                            found_flag = True

                            break

                print("filename :" + filename)
                print("Ref file name :" + reffile_to_find)
                if filename is None:
                    raise AirflowFailException(
                        "Error : file_type: " + file_type + " Not Found !! for file_period: " + file_period)

                return found_flag, filename

        file_period = self.period
        config_file_type = 'ie_workflow_config'

        perf_year, perf_month = file_period.split('_', 1)
        if int(perf_month) >= 4:
            FY = int(perf_year)
        else:
            FY = int(perf_year) - 1
        logging.info('FY : ' + str(FY))
        final_period_value = str(FY) + '_' + '04'
        config_file_period = final_period_value

        postgres_hook = PostgresHook(postgres_conn_id="cloudsql_conn_ie", schema=Variable.get('ie_cloudsql_schema'))
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        gpg = gnupg.GPG()
        gpg.encoding = 'utf-8'

        dag_name_id = self.dag_name_id
        dag_run_id = self.run_id
        workflow_name = dag_name_id
        dag_run_id = dag_run_id

        # Fetch Config file
        get_file_from_bucket(config_file_type, config_file_period, config_file_type, 'refFiles')

        # Fetch Ref/Trans files
        workflow_config_path = '/home/airflow/gcs/data/ie/' + config_file_type + '/dec/' + config_file_type + '.xlsx'
        expected_files_df = pd.read_excel(workflow_config_path)
        expected_files_df_flattened = expected_files_df.fillna(method='ffill', axis=0)
        expected_files_df = expected_files_df.query("Workflow_Name == '" + workflow_name + "'").fillna("")
        expected_files_df_flattened = expected_files_df_flattened.query("Workflow_Name == '" + workflow_name + "'").fillna("")

        for index, row in expected_files_df_flattened.iterrows():

            file_flag = row['Ref_or_Trans_File']
            logging.info('file_flag : ' + file_flag)

            file_type = row['File_Type']
            logging.info('File_Type : ' + file_type)

            if file_flag == 'Reference_File':

                found_flag, filename = get_file_from_bucket(file_type, file_period, dag_name_id, 'refFiles')
                context['task_instance'].xcom_push(key=file_type, value=found_flag)
                context['task_instance'].xcom_push(key=file_type + '_filename', value=filename)

            elif file_flag == 'Transaction_File':
                segment = XCom.get_one(
                run_id=dag_run_id,
                dag_id=dag_name_id,
                key='source_channel',
            )
                logging.info('Segment for Trans File is: '+str(segment))
                if str(segment) != None and str(segment) != 'None' and str(segment) != '':
                    if str(segment) == row['Segment']:
                        found_flag, filename = get_file_from_bucket(file_type, file_period, dag_name_id, 'transFiles')
                        context['task_instance'].xcom_push(key=file_type, value=found_flag)
                        context['task_instance'].xcom_push(key=file_type + '_filename', value=filename)

                else:
                    found_flag, filename = get_file_from_bucket(file_type, file_period, dag_name_id, 'transFiles')
                    context['task_instance'].xcom_push(key=file_type, value=found_flag)
                    context['task_instance'].xcom_push(key=file_type + '_filename', value=filename)


        ###SCHEMA VALIDATION####

        pii_list = []
        bq_list = []

        schema_flag = False

        segment = XCom.get_one(
            run_id=dag_run_id,
            dag_id=dag_name_id,
            key='source_channel',
        )

        for index, row in expected_files_df.iterrows():
            # if row['Segment'] == segment or row['Ref_or_Trans_File'] == 'Reference_File':
            if True:
                file_type = row['File_Type']
                logging.info('Schema Validation for File_Type : ' + file_type)
                expected_files_df_flattened = expected_files_df
                file_ext = row['File_Extension']

                bq_table = row['BQ_Table']
                bq_list.append(bq_table)
                path = '/home/airflow/gcs/data/ie/' + dag_name_id + '/dec/' + file_type + '.xlsx'
                
                if file_ext == 'xlsx':

                    sheets_dict = pd.read_excel(
                        '/home/airflow/gcs/data/ie/' + dag_name_id + '/dec/' + file_type + '.xlsx',
                        sheet_name=None,
                        engine='openpyxl',dtype=str)
                    expected_files_df_flattened = expected_files_df_flattened \
                        .query("Workflow_Name == '" + workflow_name + "'").fillna("")
                    expected_files_df_flattened = expected_files_df_flattened.query(
                        "File_Type == '" + file_type + "'").fillna(
                        "")

                    for index, row in expected_files_df_flattened.iterrows():
                        sheet_name = row['Sheet_Name']
                        logging.info('Expected sheet name: ' + sheet_name)
                        file_schema = row['File_Schema']
                        logging.info('Expected File_Schema for sheet name: ' + sheet_name + ' is: ' + file_schema)

                        pii_list.append(str(row['Sensitive_Columns']))
                        logging.info('Sensitive_Columns list is: ' + sheet_name + ' is: ' + file_schema)

                        for name, sheet in sheets_dict.items():
                            if name == sheet_name:
                                logging.info('Actual Sheet name = ' + str(name))
                                actual_cols = ''
                                for i, col in enumerate(sheet.columns):
                                    if i == 0:  # first column in schema
                                        actual_cols += col + ','

                                    elif i == (len(sheet.columns) - 1):  # last column in schema
                                        actual_cols += col
                                    else:
                                        actual_cols += col + ','
                                logging.info('Actual Sheet cols = ' + str(actual_cols))

                                if actual_cols == file_schema:
                                    logging.info("Schema Validation SUCCESS!!! for sheet_name: " + name)
                                    schema_flag = True
                                    context['task_instance'].xcom_push(key=file_type + '_' + name + '_schema',
                                                                       value=schema_flag)
                                    context['task_instance'].xcom_push(key=file_type + '_' + name + '_actual_schema',
                                                                       value=actual_cols)
                                else:
                                    logging.info("Schema Validation FAILED!!! for sheet_name: " + name)
                                    schema_flag = False
                                    context['task_instance'].xcom_push(key=file_type + '_' + name + '_schema',
                                                                       value=schema_flag)
                                    context['task_instance'].xcom_push(key=file_type + '_' + name + '_actual_schema',
                                                                       value=actual_cols)


        ###CLEAN HEADERS AND REGENERATE###

        client = bigquery.Client()
        dataset = Variable.get('ie_bq_gold_dataset')
        bq_project = Variable.get('ie_project_id')

        expected_files_df_flattened = expected_files_df
        expected_files_df_flattened = expected_files_df_flattened \
            .query("Workflow_Name == '" + workflow_name + "'").fillna("")

        for index, row in expected_files_df_flattened.iterrows():
            # if row['Segment'] == segment or row['Ref_or_Trans_File'] == 'Reference_File':
            if True:
                file_type = row['File_Type']
                logging.info('Schema Validation for File_Type : ' + file_type)

                bq_table = row['BQ_Table']

                if dag_name_id == 'ie_reference_validation':
                    # Calculate FY from PERIOD
                    perf_year, perf_month = file_period.split('_', 1)
                    if int(perf_month) >= 4:
                        FY = int(perf_year) + 1
                    else:
                        FY = perf_year
                    logging.info('FY : ' + str(FY))
                    bq_table = row['BQ_Table'].replace('{fy}', str(FY))

                    expected_files_df_flattened.at[index, 'BQ_Table'] = bq_table
                    expected_files_df.at[index, 'BQ_Table'] = bq_table

                    context['task_instance'].xcom_push(key='year', value=str(FY))

                logging.info('bq_table : ' + bq_table)

                file_ext = row['File_Extension']

                if file_ext == 'xlsx':
                    outdir = '/home/airflow/gcs/data/ie/' + dag_name_id + '/modified/'
                    if not os.path.exists(outdir):
                        os.makedirs(outdir)
                    tail = file_type + '.xlsx'

                    all_sheets = []

                    sheets_dict = pd.read_excel(
                        '/home/airflow/gcs/data/ie/' + dag_name_id + '/dec/' + file_type + '.xlsx',
                        sheet_name=None,
                        engine='openpyxl',dtype=str)

                    for name, sheet in sheets_dict.items():
                        sheet['sheet'] = name
                        sheet.columns = [x.lower() for x in sheet.columns]
                        sheet.columns = [x.replace("\n", " ") for x in sheet.columns.to_list()]
                        sheet = sheet.rename(columns=lambda x: x.split('\n')[-1])
                        sheet = sheet.loc[:, ~sheet.columns.duplicated()].copy()
                        all_sheets.append(sheet)

                    df = pd.concat(all_sheets)
                    # df.fillna('')
                    df = df.loc[:, ~df.columns.duplicated()].copy()
                    df.reset_index(inplace=True, drop=True)
                    df['sheet_name'] = df['sheet']
                    df['file_name'] = '/home/airflow/gcs/data/ie/' + dag_name_id + '/dec/' + file_type + '.xlsx'
                    del df['sheet']

                    df.rename(columns=lambda x: x.strip())
                    df.columns = df.columns.str.replace('%', 'percent')
                    df.columns = [re.sub('[^A-Za-z0-9_]+', '_', str(c)) for c in df.columns]
                    df.columns = df.columns.str.replace('__', '_')
                    df.columns = [re.sub('_$', '', str(c)) for c in df.columns]

                    df.fillna('nan')
                    # Handling ID Columns adding Identifier
                    for c in df.columns:
                        if (c.startswith('id_')) or (c.startswith('n_')) or (c.startswith('num')):
                            df[c] = '@IDT@' + df[c].astype(str)

                    # drop empty unexp cols
                    try:
                        df.drop([""], axis=1, inplace=True)
                    except:
                        pass

                    cols = pd.Series(df.columns)

                    # rename duplicate columns
                    for dup in cols[cols.duplicated()].unique():
                        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in
                                                                         range(sum(cols == dup))]

                    df.columns = cols
                    df = df.replace(r'\n',' ', regex=True)
                    tail = os.path.splitext(tail)[0] + '.txt'
                    df.to_csv(outdir + tail, index=None, sep='\t')

                    logging.info("Deleting existing BQ Data from table: " + str(bq_table))

                    try:
                        QUERY = "Delete from `" + bq_project + "." + dataset + "." + str(bq_table) + "` where commission_month="+"'"+file_period+"'"
                        logging.info('QUERY : ' + QUERY)
                        query_job = client.query(QUERY)

                    except Exception:
                        logging.info('Query failed or table does not exist')
                        # logging.info(Exception)


        
        ###TRIGGER DYNAMIC DF####

        timestamp_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        batch_id = self.batch_id
        latest_key_name = self.latest_key_name
        file_period=self.period

        logging.info("pii list: " + str(pii_list))
        cols = []
        categoryConfig = ''
        categoryKeyConfig = ''
        categoryAdditionalData = ''

        for i in pii_list:
            if i != '':
                i = i.lower().replace("\n", " ").strip().replace('%', 'percent')
                i = re.sub('[^A-Za-z0-9_,]+', '_', i)
                i.replace('__', '_')
                i = re.sub('_$', '', i)
                temp_list = i.split(",")
                for t in temp_list:
                    cols.append(t.replace(' ', ''))

        logging.info("All sensitive cols: " + str(cols))

        for j in cols:
            if len(cols) == 1:
                categoryConfig += str(j) + ':' + str(j) + '_alias'
                categoryKeyConfig += str(j) + '_alias:' + '${ENC_DEC_pass}'
                categoryAdditionalData += str(j) + '_alias:' + str(j)

            else:
                categoryConfig += str(j) + ':' + str(j) + '_alias,'
                categoryKeyConfig += str(j) + '_alias:' + '${ENC_DEC_pass},'
                categoryAdditionalData += str(j) + '_alias:' + str(j) + ','

        logging.info('category_config :' + categoryConfig)
        logging.info('categoryKeyConfig :' + categoryKeyConfig)

        file_directive = ''
        pl_name = 'ie_generic_txt_file_load'
        file_directive += 'parse-as-csv :body \'\\t\' true\n'
        file_directive += 'drop :body\n'
        file_directive += 'set-column batch_id ' + '\'' + batch_id + '\'\n'
        file_directive += 'set-column timestamp ' + '\'' + timestamp_value + '\'\n'
        file_directive += 'set-column commission_month ' + '\'' + file_period + '\'\n'
        file_directive += 'parse-as-datetime :timestamp \'yyyy-MM-dd HH:mm:ss\''

        expected_files_df_drop = expected_files_df.drop_duplicates(subset=['File_Type'])

        for index, row in expected_files_df_drop.iterrows():
            # if row['Segment'] == segment or row['Ref_or_Trans_File'] == 'Reference_File':
            if True:
                file_type = row['File_Type']
                filepath = '/home/airflow/gcs/data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
                filepath_gcs = 'gs://' + Variable.get(
                    'ie_dag_bucket') + '/data/ie/' + dag_name_id + '/modified/' + file_type + '.txt'
                bigquery_table = str(row['BQ_Table'])

                file_cols = pd.read_csv(filepath, sep='\t').columns.tolist()
                file_cols.insert(0, "batch_id")
                file_cols.insert(1, "timestamp")
                file_cols.insert(2, "commission_month")
                appended_string = ''
                pre_string = '{\"type\":\"record\",\"name\":\"record\",\"fields\":['
                post_string = ']}'

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
                                  'file_period':file_period,
                                  'ie_bq_table_name': bigquery_table,
                                  'ie_bucket': Variable.get('ie_bucket'),
                                  'ie_bq_dataset': Variable.get('ie_bq_gold_dataset'),
                                  'ie_bq_region': Variable.get('ie_bq_region'),
                                  'vault_path': Variable.get('vault_path_ie'),
                                  'DEC_KEY_GPG': latest_key_name,
                                  'field_categ': categoryConfig,
                                  'categ_key': categoryKeyConfig,
                                  'add_data': categoryAdditionalData
                                  },
                    asynchronous=True,
                    gcp_conn_id=Variable.get('gcp_conn_id'),
                    pipeline_timeout=20 * 60,
                )
                try:
                    trigger_pipeline.execute(dict())
                except:
                    pass

            context['task_instance'].xcom_push(key='categoryConfig', value=str(categoryConfig))
            context['task_instance'].xcom_push(key='categoryKeyConfig', value=str(categoryKeyConfig))
            context['task_instance'].xcom_push(key='categoryAdditionalData', value=str(categoryAdditionalData))

        wait_task = PythonOperator(
            task_id="wait_task",
            python_callable=lambda: time.sleep(260),
            trigger_rule="all_success",
        )
        wait_task.execute(dict())



        ###CHECK BQ TABLES###

        # for i in expected_files_df['BQ_Table']:
        for index, row in expected_files_df_drop.iterrows():
            i = row['BQ_Table']
            # if row['Segment'] == segment or row['Ref_or_Trans_File'] == 'Reference_File':
            if True:
                count = 0
                query = "SELECT  count(1) as count FROM `" + bq_project + "." + dataset + "." + i + "`"
                logging.info("Query: " + query)
                query_job = client.query(query)
                rows = query_job.result()

                for row in rows:
                    count = row.count
                    logging.info("Count is: " + str(count))

                if count >= 1:
                    logging.info("Data Found for table: " + i)
                    context['task_instance'].xcom_push(key=i, value=str(count))

                    continue
                elif "exclusion" in i.lower():
                    logging.info("No Data Found for table: " + i)
                    context['task_instance'].xcom_push(key=i, value=str(count))
                    
                    continue
                elif "second" in i.lower():
                    logging.info("No Data Found for table: " + i)
                    context['task_instance'].xcom_push(key=i, value=str(count))

                else:
                    context['task_instance'].xcom_push(key=i, value=str(count))
                    raise AirflowFailException(" No Data Found!!! for table..." + i)