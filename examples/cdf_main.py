from typing import Any
import composer2_airflow_rest_api

def trigger_dag_gcf(data, context=None):
    #define airflow instance url as per env
    web_server_url = (
        #"https://99ecf6b27f39428b882f48b02a3ba4c9-dot-europe-west1.composer.googleusercontent.com"
        "https://2836bd65dd584c97b35bb1334106efca-dot-europe-west1.composer.googleusercontent.com/"
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = 'ie_cf_trigger'
    #call the api for composer connection
    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)
