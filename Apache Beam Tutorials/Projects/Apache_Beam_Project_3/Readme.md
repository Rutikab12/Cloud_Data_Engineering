#GCP Data Engineering Project: Building and Orchestrating an ETL Pipeline for Online Food Delivery Industry with Apache Beam and Apache Airflow

This GCP Data Engineering project focuses on developing a robust ETL (Extract, Transform, Load) pipeline for the online food delivery industry. The pipeline is designed to handle batch transactional data and leverages various Google Cloud Platform (GCP) services:
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
🗃️GCS is used to store and manage the transactional data
⭐Composer, a managed Apache Airflow service, is utilized to orchestrate Dataflow jobs
🌊Dataflow, based on Apache Beam, is responsible for data processing, transformation, and loading into BigQuery
🔍BigQuery serves as a serverless data warehouse
📊Looker, a business intelligence and analytics platform, is employed to generate daily reports
These technologies work together to efficiently process, store, and generate reports on the daily transaction data.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
🗃️GCS
Upload the provided CSV file to your designated Google Cloud Storage (GCS) bucket. This transactional data represents a sample of real-world cases from the online food delivery industry. It includes information such as customer ID, date, time, order ID, items ordered, transaction amount, payment mode, restaurant name, order status, customer ratings, and feedback. The data showcases various scenarios, including late delivery, stale food, and complicated ordering procedures, providing valuable insights into different aspects of the customer experience.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
🐝Beam code

beam.py code is a data processing pipeline implemented using Apache Beam. It reads data from an input file, performs cleaning and filtering operations, and writes the results to two separate BigQuery tables based on specific conditions.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
The pipeline consists of the following steps:

Command-line arguments are parsed to specify the input file.
The data is read from the input file and undergoes cleaning operations, such as removing trailing colons and special characters.
The cleaned data is split into two branches based on the status of the orders: delivered and undelivered.
The total count of records, delivered orders count, and undelivered orders count are computed and printed.
The cleaned and filtered data from the delivered orders branch is transformed into JSON format and written to a BigQuery table.
Similarly, the cleaned and filtered data from the undelivered orders branch is transformed into JSON format and written to another BigQuery table.
The pipeline is executed, and the success or failure status is printed.
👩‍💻
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
⭐️Composer/Airflow
📖

The DAG monitors the GCS bucket for new files with the specified prefix using the GoogleCloudStoragePrefixSensor (for Airflow 1) or GCSObjectsWithPrefixExistenceSensor (for Airflow 2). When a new file is found, it executes the list_files function which uses the GoogleCloudStorageHook (for Airflow 1) and GCSHook (for Airflow 2) to move the file to a 'processed' subdirectory and delete the original file. Finally, it triggers the execution of a Dataflow pipeline using the DataFlowPythonOperator (for Airflow 1) or DataflowCreatePythonJobOperator/BeamRunPythonPipelineOperator (for Airflow 2) with the processed file as input.

This setup is ideal for recurring data processing workflows where files arrive in a GCS bucket at regular intervals (e.g., every 10 minutes) and need to be transformed using Dataflow and loaded into BigQuery. By using Apache Airflow and this DAG, you can automate and schedule the data processing workflow. The DAG ensures that the tasks are executed in the defined order and at the specified intervals.

Do note that the actual operator and hook names, and some of their parameters, will differ between Airflow 1 and Airflow 2. Be sure to use the correct names and parameters for your version of Airflow. For example, if your code contains contrib imports, it can only be run in Composer 1.

For more information about Airflow operators, please refer to the official Apache Airflow documentation at https://airflow.apache.org/ or the Astronomer Registry at https://registry.astronomer.io/. Additionally, if you have any specific questions or need further guidance, you can interact with “Ask Astro” an LLM-powered chatbot, available at https://ask.astronomer.io.

To gain a better understanding of the process, review the logs of each individual task.

🚀 gcs_sensor
Sensor checks existence of objects: food-orders-us, food_daily. Success criteria met. Sensor found the file in the bucket.

🚀 list_files
Object food_daily.csv in bucket food-orders-us copied to object processed/food_daily.csv in bucket food-orders-us. Blob food_daily.csv deleted.

🚀 beamtask
The Dataflow job is triggered and executed using this task.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Looker
Connect to your Looker account: https://lookerstudio.google.com. Select BQ connection. Create your own daily report, use delivered/other_status_orders tables. 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Set the project in the cloud shell: gcloud config set project your-project-id

Install Apache Beam in the cloud shell: pip install apache-beam[gcp]

Give the Beam code a test run in the shell and then check the results in BigQuery: python beam.py --input gs://your-bucket/food_daily.csv --temp_location gs://your-bucket

❗ Make sure that all your files and services are in the same location. E.g. both buckets should be in the same location or you will get a similar error message: ‘Cannot read and write in different locations: source: US, destination: EU’

To avoid any confusion, it is recommended to delete the dataset before moving forward with actions that involve appending data in BigQuery.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Project Cloned from Article of
https://medium.com/google-cloud/%EF%B8%8Fgcp-data-engineering-project-building-and-orchestrating-an-etl-pipeline-for-online-food-delivery-0fc8c532be14