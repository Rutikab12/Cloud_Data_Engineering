import logging
from datetime import datetime
from airflow.models import Variable

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import re
from airflow.exceptions import AirflowFailException

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

def trigger_dag_op(dag_id,period):
    trigger_dag = TriggerDagRunOperator(
        task_id="generate_excel_and_encrypt",
        trigger_dag_id=dag_id,
        conf={
            'period':period
        },
        wait_for_completion=False,
    )
    trigger_dag.execute(dict())


def parse_context_cf(**kwargs):
    #trans_file_type = Variable.get('ie_trans_file_type')
    transaction_data_path='inbound/EXT1507_COMMISSIONS_EVENTS/'
    product_data_path='inbound/EXT1529_PRODUCT_CATALOGUE/'
    employee_data_path='inbound/EXT1529_EMPLOYEE_REFERENCE/'
    #ref_file_type = Variable.get('ie_ref_file_type')
    conf = kwargs['dag_run'].conf
    print(f"conf value is: {conf}")
    message_data = conf['message']
    attributes = message_data['attributes']
    print(f"attributes are: {attributes}")
    fetched_object_path = attributes['objectId']
    print(str(fetched_object_path))
    
    if fetched_object_path.endswith('/') or fetched_object_path.endswith('_SUCCESS'):
        raise AirflowFailException('Invalid File Location!!!')
    else:
        if transaction_data_path in fetched_object_path:
            dag_to_trigger = 'ie_transactions_data_ingestion'
            period = '2025_02'
            try:
                trigger_dag_op(dag_to_trigger,period)
            except:
                pass

        elif product_data_path in fetched_object_path:
            dag_to_trigger = 'ie_product_catalogue_dynamo_ingestion'
            #period = match.group(0)
            period='2025_02'
            try:
                trigger_dag_op(dag_to_trigger,period)
            except:
                pass
      
        elif employee_data_path in fetched_object_path:
            dag_to_trigger = 'ie_dealer_hierarchy_dynamo_ingestion'
            #period = match.group(0)
            period='2025_02'
            try:
                trigger_dag_op(dag_to_trigger,period)
            except:
                pass

        else:
            logging.info("File in not part of Automated Ingestion!")
            raise AirflowFailException('Dynamo File not found in uploaded folder!!!')
    
    #match = re.search(r'\d{4}_\d{2}', fetched_object_path)

    '''if not transaction_data_path in fetched_object_path or not product_data_path in fetched_object_path:
        raise AirflowFailException('File not found in uploaded folder!!!')'''
    


with DAG(
        'ie_cf_trigger',
        default_args=default_args,
        # description="{\"name\":\"ie_cf_trigger\",\"description\":\"**This workflow executes pipeline for target transformation.**\\n**It takes below inputs:-**\\n1. Read the raw data from BigQuery\\n2. Reference files uploaded from Portal and generates XCalc table and various Reports as output.\\n\\n**This workflow has below tasks:-**\\n1. Get parameter values\\n2. Basis parameter values fetch reference/ transaction files from Cloud Storage.\\n3. Trigger Pipeline for target transformation in Cloud Data Fusion.\\n4. Generate Reports on Cloud Storage in excel format.\",\"fields\":[{\"name\":\"period\",\"type\":\"date\",\"label\":\"Period\",\"format\":\"yyyy_MM\"}]}",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        schedule_interval=None,
        max_active_runs=1,
        tags=['ie', 'ie_validation'],
) as dag:

    parse_context = PythonOperator(
        task_id="parse_context",
        python_callable=parse_context_cf,
        provide_context=True,
    )