from googleapiclient.discovery import build


def trigger_df_job(cloud_event, environment):
    service = build('dataflow', 'v1b3')
    project = "cricbuzzproject"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bqloadtest",  # Provide a unique name for the job
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://dataflow-metadata-bucket/udf.js",
            "JSONPath": "gs://dataflow-metadata-bucket/bq.json",
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "cricbuzzproject:cricket_dataset.icc_odi_batsman_ranking",
            "inputFilePattern": "gs://bkt-cricket-ranking/batsman_rankings.csv",
            "bigQueryLoadingTemporaryDirectory": "gs://dataflow-metadata-bucket",
        }
    }

    request = service.projects().templates().launch(projectId=project, gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)