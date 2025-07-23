def copy_payees_to_bigquery(**kwargs):
    timestamp_value = kwargs['timestamp_value']
    batch_id = kwargs['batch_id']
    project_id = Variable.get('ie_project_id')
    bq_dataset = Variable.get('ie_bq_staging_dataset')
    bq_table_name = 'payees'
    pg_schema = Variable.get('cloudsql_schema_ie')
    pg_table_name = 'payees'
    
    # Establish connections
    pg_hook = PostgresHook(postgres_conn_id='cloudsql_conn_ie')
    bq_client = bigquery.Client(project=project_id)
    
    try:
        # Extract data from PostgreSQL
        with pg_hook.get_conn() as pg_conn, pg_conn.cursor() as cursor:
            # Get filtered data from payees table
            cursor.execute(f"""
                SELECT * FROM "{pg_schema}"."{pg_table_name}" 
                WHERE lower("a01") = 'direct' 
                AND "isApproved" = '1'
            """)
            rows = cursor.fetchall()
            
            # Get column names and types
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{pg_schema}'
                AND table_name = '{pg_table_name}'
                ORDER BY ordinal_position
            """)
            columns_info = cursor.fetchall()
            column_names = [col[0] for col in columns_info]
            
            print(f"Fetched {len(rows)} rows from PostgreSQL payees table")
        
        # Prepare BigQuery table reference
        bq_table_ref = bq_client.dataset(bq_dataset).table(bq_table_name)
        
        # Create schema for BigQuery (using standard SQL types)
        schema = []
        for col_name, col_type in columns_info:
            if col_type in ('integer', 'bigint', 'smallint'):
                bq_type = 'INT64'
            elif col_type in ('numeric', 'decimal'):
                bq_type = 'NUMERIC'
            elif col_type in ('timestamp', 'timestamp with time zone'):
                bq_type = 'TIMESTAMP'
            elif col_type == 'date':
                bq_type = 'DATE'
            elif col_type == 'boolean':
                bq_type = 'BOOL'
            else:
                bq_type = 'STRING'
            schema.append(bigquery.SchemaField(col_name, bq_type))
        
        # Configure load job with WRITE_TRUNCATE
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        # Convert data to JSON lines format
        import json
        from io import StringIO
        json_rows = StringIO()
        for row in rows:
            json_rows.write(json.dumps(dict(zip(column_names, row)) + '\n')
        json_rows.seek(0)
        
        # Load data to BigQuery (this will replace existing data)
        load_job = bq_client.load_table_from_file(
            json_rows,
            bq_table_ref,
            job_config=job_config
        )
        
        # Wait for job to complete
        load_job.result()
        
        # Check for errors
        if load_job.errors:
            print(f"Encountered errors while loading data: {load_job.errors}")
            raise Exception("BigQuery load errors occurred")
        else:
            print(f"Successfully loaded {load_job.output_rows} rows to BigQuery")
            print(f"Table {bq_dataset}.{bq_table_name} was overwritten with new data")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e
