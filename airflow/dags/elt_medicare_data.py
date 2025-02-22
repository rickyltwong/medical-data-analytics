from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

    s3_hook = S3Hook(aws_conn_id='s3_default')

    @task(retries=3)
    def get_offset_count():
        try:
            # check if the file exists
            if not s3_hook.check_for_key(
                key='medicare/metadata/last_spending_extract.txt',
                bucket_name='pharmaceutical-data-dashboard'
            ):
                return 0

            # read the file
            offset = s3_hook.read_key(
                key='medicare/metadata/last_spending_extract.txt',
                bucket_name='pharmaceutical-data-dashboard'
            )

            logging.info(f"Offset count: {offset}")

            if offset is None:
                return 0
            else:
                return int(offset)
                
        except Exception as e:
            logging.error(f"Error accessing S3: {str(e)}")
            # If we can't access the offset file, start from 0
            return 0
    
    @task
    def extract_spending_data(offset: int):

        params = {  
            "limit": 100,
            "offset": offset
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
            key=f'medicare/spending_data/spending_data_{offset}_{pendulum.now().strftime("%Y-%m-%d")}.json',
            bucket_name='pharmaceutical-data-dashboard',
            replace=False
        )

        s3_hook.load_string(
            string_data=str(offset + len(data)),
            key='medicare/metadata/last_spending_extract.txt',
            bucket_name='pharmaceutical-data-dashboard',
            replace=True
        )

        # Log the offset count
        logging.info(f"Updated offset count to: {offset + len(data)}")

    @task
    def validate_spending_data():

        # read the data from s3
        data = s3_hook.read_key()

        # validate the data



    # Define the DAG
    extract_spending_data(get_offset_count())

medicare_data_pipeline()