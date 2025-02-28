# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping
# from airflow.hooks.base import BaseHook
# import json
# import os
# from datetime import datetime

# # Get S3 credentials from Airflow connection
# s3_conn = BaseHook.get_connection('s3_default')
# s3_extra = json.loads(s3_conn.extra or '{}')

# # Get Snowflake credentials
# snowflake_conn = BaseHook.get_connection('snowflake_default')

# profile_config = ProfileConfig(
#     profile_name="default",
#     target_name="dev",
#     profile_mapping=SnowflakeUserPasswordProfileMapping(
#         conn_id="snowflake_default",
#         profile_args={
#             "schema": "medicare",
#             "database": "MEDICARE_DB",
#             "warehouse": "COMPUTE_WH"
#         },
#     ),
# )

# # Set environment variables for external stage
# os.environ['SPACES_KEY'] = s3_conn.login
# os.environ['SPACES_SECRET'] = s3_conn.password
# os.environ['SPACES_ENDPOINT'] = s3_extra.get('host', 'https://tor1.digitaloceanspaces.com')

# dbt_dag = DbtDag(
#     project_config=ProjectConfig(
#         "dbt/medicare_analytics_dbt",
#     ),
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(
#         dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
#     ),
#     # normal dag parameters
#     schedule_interval="@daily",
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     dag_id="dbt_dag",
#     default_args={"retries": 2},
# )