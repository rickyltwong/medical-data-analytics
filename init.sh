#! /bin/bash

PROJECT_DIR=$(pwd)

cd ${PROJECT_DIR}/standalone-spark
docker compose build
docker compose up -d --scale worker=2

cd ${PROJECT_DIR}/airflow
docker compose build
docker compose up airflow-init && docker compose up -d --scale airflow-init=0

cd ${PROJECT_DIR}

docker network create shared_network

docker network connect shared_network standalone-spark-master-1

docker network connect shared_network standalone-spark-worker-1

docker network connect shared_network standalone-spark-worker-2

docker network connect shared_network airflow-webserver-1

docker network connect shared_network airflow-scheduler-1

docker network connect shared_network airflow-worker-1

# check http://localhost:18080/
# check http://localhost:8080/connection/list/
# test run http://localhost:8080/dags/test_pyspark

docker run -d -p 3000:3000 --name metabase metabase/metabase

# check http://localhost:3000/
# metabase does not connect to the shared network because it is connected to the snowflake.