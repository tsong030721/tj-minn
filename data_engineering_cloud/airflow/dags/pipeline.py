from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# AWS Configuration
LAMBDA_FUNCTION_NAME = "nba_data_fetch"
S3_BUCKET = "nba-data-storage"
S3_DATA_PATH = "last_10_team_"  # Prefix for saved data files
GLUE_JOB_NAME = "s3_to_redshift"


# Define DAG
dag = DAG(
    "nba_data_pipeline",
    schedule_interval="0 12 * * *",  # Runs daily at noon UTC
    start_date=days_ago(1),
    catchup=False,
)

# Trigger AWS Lambda to fetch NBA data and store in S3
fetch_nba_data = LambdaInvokeFunctionOperator(
    task_id="fetch_nba_data",
    function_name=LAMBDA_FUNCTION_NAME,
    aws_conn_id="aws_default",
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# Wait until data appears in S3
wait_for_s3_data = S3KeySensor(
    task_id="wait_for_s3_data",
    bucket_name=S3_BUCKET,
    bucket_key=f"{S3_DATA_PATH}*",  # Waits for at least one file with this prefix
    timeout=60 * 10,  # 10-minute timeout
    poke_interval=60,  # Check every minute
    aws_conn_id="aws_default",
    dag=dag,
)

# Run AWS Glue transformation job
transform_nba_data = GlueJobOperator(
    task_id="transform_nba_data",
    job_name=GLUE_JOB_NAME,
    aws_conn_id="aws_default",
    dag=dag,
)

# Task dependencies
fetch_nba_data >> wait_for_s3_data >> transform_nba_data
