from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.operators.python import PythonOperator
from pyathena import connect
from pyathena.pandas_cursor import PandasCursor

import os
import boto3

# Get the DAG ID from the file name
DAG_ID=os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS={
            # Default arguments for the DAG
            # the number of retries that should be performed before failing the task
            "owner": "bhakti",
            "retries": 1,
            "email_on_failure": False,
            "email_on_retry": False,
        }
# Glue job configuration
job_name = "data-transformation"
region_name= "us-east-1"
iam_role_name="demo-mwaa-glue"

config = {
    "Name":"catalog-data",
    "Role": "demo-mwaa-glue",
    "DatabaseName":"curated-data-2024",
    "Description":"Crawl dataset and catalog the the data",
    'Targets':{'S3Targets' : [{'Path': "s3://myairflowdemo2024/curated-data-2024/" }]}
}

with DAG(
        dag_id= DAG_ID,   
        description='Prepare data pipeline orchestration demo',
        default_args = DEFAULT_ARGS,
        start_date=datetime(2023, 4, 28),
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=10),
        catchup=False,
        tags=["Data Pipeline Orchestration"]
) as dag:
    # Define the tasks
    
    # Dummy task to start the DAG
    begin = DummyOperator(task_id="begin")

    # Dummy task to end the DAG 
    end = DummyOperator(task_id="end")

    def check_data_quality():
    # Check if file exists in S3 bucket
        s3 = boto3.client('s3')
        bucket_name = 'myairflowdemo2024'  # replace with your bucket name
        key = 'landed-zone-2024/Childcarecentres.csv'  # replace with your file path
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            print("File exists in S3 bucket")
        except Exception as e:
            print("File does not exist in S3 bucket")

    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
        dag=dag,
    )
    
    # Task to purge processed data from S3 bucket
    purge_processed_data_s3_objects = BashOperator(
        task_id="purge_processed_data_s3_objects",
        bash_command=f'aws s3 rm s3://myairflowdemo2024/processed-data-2024/ --recursive',
    )
    
    # Task to purge data catalog in Glue    
    purge_data_catalog = BashOperator(
        task_id="purge_data_catalog",
        bash_command='aws glue delete-table --database-name curated-data-2024 --name curated_data-2024 || echo "Database -details not found."',
    )
    
    # Task to run the Glue Job
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name=job_name,
        region_name= region_name,
        script_location="s3://myairflowdemo2024/scripts-2024/etlscript.py",
        s3_bucket="airflowmwaa-demo",
        iam_role_name=iam_role_name,
        aws_conn_id="aws_default",
        create_job_kwargs={"GlueVersion": "3.0",
                           "WorkerType": "G.1X",
                           "NumberOfWorkers": 4,},      
    )
    
    # Task to run the Glue Crawler
    run_glue_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        aws_conn_id= "aws_default",
        config=config,       
    )
    
    # Task to sync buckets in S3
    sync_buckets = BashOperator(
        task_id="sync_buckets",
        bash_command='aws s3 sync s3://myairflowdemo2024/landed-zone-2024/  s3://myairflowdemo2024/processed-data-2024/',
    )
    
    # Task to purge raw data files from S3 bucket
    purge_raw_data_file = S3DeleteObjectsOperator(
        task_id="purge_raw_data_file",
        bucket="myairflowdemo2024",
        keys=["landed-zone-2024/Childcarecentres.csv"],
        aws_conn_id="aws_default",
    ) 

    def check_data_quality_athena(*args, **kwargs):
    # Connect to Athena
        conn = connect(s3_staging_dir='s3://myairflowdemo2024/curated-data-2024',
                    region_name=region_name,  # replace with your region
                    cursor_class=PandasCursor)

        # Check if table has columns 'Latitude' and 'Longitude'
        df = conn.cursor().execute('SELECT * FROM "curated-data-2024"."curated_data_2024" limit 10;').fetch_pandas_all()  # replace with your table name
        if 'latitude' in df.columns and 'longitude' in df.columns:
            print("Table has columns 'Latitude' and 'Longitude'")
        else:
            print("Table does not have columns 'Latitude' and 'Longitude'")

    quality_check_athena = PythonOperator(
        task_id='data_quality_check_athena',
        python_callable=check_data_quality_athena,
        dag=dag,
    )
# Define the task dependencies using chain    
chain(
    begin,
    (quality_check),
    (purge_processed_data_s3_objects,purge_data_catalog),
    (run_glue_job),
    (run_glue_crawler),
    (sync_buckets),
    (purge_raw_data_file),
    (quality_check_athena),
    end
)