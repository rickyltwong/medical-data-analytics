from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
import requests
import json
import pendulum
import logging

@dag(
    description="Incremental Medicare data pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 2, 14, tz="UTC"),
)
def medicare_data_pipeline():

    aws_conn = BaseHook.get_connection('s3_default')
    s3_hook = S3Hook(aws_conn_id='s3_default')

    @task(retries=3)
    def get_offset_count():
        try:
            files = s3_hook.list_keys(
                bucket_name='pharmaceutical-data-dashboard',
                prefix='medicare/spending_data/spending_data_'
            )

            if not files:
                return 0

            max_offset = 0
            for file in files:
                try:
                    parts = file.split('/')[-1].split('_')
                    end_offset = int(parts[3].split('.')[0])
                    max_offset = max(max_offset, end_offset)
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipping malformed filename {file}: {str(e)}")
                    continue

            logging.info(f"Latest offset count from files: {max_offset}")
            return max_offset

        except Exception as e:
            raise Exception(f"Error accessing S3: {str(e)}")
    
    @task
    def extract_spending_data(offset: int):

        params = {  
            "offset": offset,
            "size": 1000
        }

        # Extract spending data from API
        api_url = "https://data.cms.gov/data-api/v1/dataset/7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b/data"

        response = requests.get(api_url, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")
        
        data = response.json()

        # validate the count of data
        if len(data) == 0:
            raise Exception("No data found in the response")
        
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=f'medicare/spending_data/spending_data_{offset}_{offset + len(data)}_{pendulum.now().strftime("%Y-%m-%d")}.json',
            bucket_name='pharmaceutical-data-dashboard',
            replace=False
        )

        # Log the offset count
        logging.info(f"Updated offset count to: {offset + len(data)}")

    unpivot_medicare_spending_data = SparkSubmitOperator(
            task_id='unpivot_medicare_spending_data',
            application='dags/scripts/unpivot_medicare_spending_data.py',
            conn_id='spark_default',
            packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261',
            conf={
            "spark.driver.bindAddress": "0.0.0.0",
            # "spark.driver.host": "172.20.0.6", # airflow's worker pod IP
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'tor1.digitaloceanspaces.com',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.access.key': '{{ conn.s3_default.login }}',
            'spark.hadoop.fs.s3a.secret.key': '{{ conn.s3_default.password }}',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.memory.fraction': '0.7',
            'spark.memory.storageFraction': '0.3',
            'spark.shuffle.file.buffer': '1mb',
            'spark.shuffle.spill.compress': 'true',
            'spark.shuffle.compress': 'true',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s'
        }
    )

    extract_spending_data(get_offset_count()) >> unpivot_medicare_spending_data

medicare_data_pipeline()