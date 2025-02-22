from pyspark.sql import SparkSession
import requests
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

spark = SparkSession.builder.appName("MedicarePrescriberExtract").getOrCreate()

s3_hook = S3Hook(aws_conn_id='do_spaces_default')

def extract_prescriber_data():
    api_url = "https://data.cms.gov/data-api/v1/dataset/<dataset-id>/data"
    
    try:
        last_processed = s3_hook.read_key(
            key='medicare/metadata/last_prescriber_extract.txt',
            bucket_name='pharmaceutical-data-dashboard'
        )
    except:
        last_processed = "2024-01-01"
    
    params = {
        "offset": 0,
        "size": 100,
        "updated_after": last_processed
    }
    
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        
        df = spark.createDataFrame(data)
        
        current_date = datetime.now().strftime("%Y%m%d")
        output_path = f"medicare/prescriber/dt={current_date}"
        
        df.write.parquet(
            f"s3a://pharmaceutical-data-dashboard/{output_path}",
            mode="append"
        )
        
        s3_hook.load_string(
            string_data=datetime.now().isoformat(),
            key='medicare/metadata/last_prescriber_extract.txt',
            bucket_name='pharmaceutical-data-dashboard',
            replace=True
        )
        
        print(f"Successfully extracted {df.count()} records")
    else:
        raise Exception(f"API request failed with status {response.status_code}")

if __name__ == "__main__":
    extract_prescriber_data()
    spark.stop() 