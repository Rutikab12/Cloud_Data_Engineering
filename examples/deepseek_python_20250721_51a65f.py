from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

#function to implement type-2 query on payees_otc_table
def copy_payees_to_bigquery(**kwargs):
    timestamp_value = kwargs['timestamp_value']
    batch_id=kwargs['batch_id']
    project_id = Variable.get('ie_project_id')
    bq_dataset = Variable.get('ie_bq_staging_dataset')
    bq_table_name = 'payees'
    pg_schema = Variable.get('cloudsql_schema_ie')
    pg_table_name = 'payees'
    
    # Establish connections
    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_ie')
    bq_client = bigquery.Client(project=project_id)
    
    try:
        #Extract data from PostgreSQL
        with pg_hook.get_conn() as pg_conn, pg_conn.cursor() as cursor:
            # Get all data from payees table
            cursor.execute(f"""SELECT * FROM "public"."{pg_table_name}" where lower("a01")='direct' and "isApproved"='1'""")
            rows = cursor.fetchall()
            
            # Get column names and types
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{pg_table_name}'
                ORDER BY ordinal_position
            """)
            columns_info = cursor.fetchall()
            column_names = [col[0] for col in columns_info]
            
            print(f"Fetched {len(rows)} rows from PostgreSQL payees table")
        
        #Prepare BigQuery table
        bq_table_ref = bq_client.dataset(bq_dataset).table(bq_table_name)
        
        # Create schema for BigQuery
        schema = []
        for col_name, col_type in columns_info:
            # Map PostgreSQL types to BigQuery types
            if col_type in ('integer', 'bigint', 'smallint'):
                bq_type = 'INT64'
            elif col_type in ('numeric', 'decimal'):
                bq_type = 'FLOAT64'
            elif col_type in ('timestamp', 'timestamp with time zone'):
                bq_type = 'TIMESTAMP'
            elif col_type == 'date':
                bq_type = 'DATE'
            else:  # Includes varchar, text, char, etc.
                bq_type = 'STRING'
            schema.append(bigquery.SchemaField(col_name, bq_type))
        print(schema)
        
        # Create or update table
        try:
            bq_client.get_table(bq_table_ref)
            print(f"BigQuery table {bq_dataset}.{bq_table_name} already exists")
        except Exception:
            table = bigquery.Table(bq_table_ref, schema=schema)
            bq_client.create_table(table)
            print(f"Created BigQuery table {bq_dataset}.{bq_table_name}")
        
        #Load data to BigQuery
        #Convert rows to list of dictionaries
        rows_to_insert = [dict(zip(column_names, row)) for row in rows]
        
        errors = bq_client.insert_rows_json(bq_table_ref, rows_to_insert)
        '''job_config = bigquery.LoadJobConfig(schema=schema)
        job = bq_client.load_table_from_dataframe(
            pd.DataFrame(rows_to_insert), 
            bq_table_ref, 
            job_config=job_config
        )
        job.result()'''
        
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
            raise Exception("BigQuery insert errors occurred")
        else:
            print(f"Successfully copied {len(rows_to_insert)} rows to BigQuery")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e
