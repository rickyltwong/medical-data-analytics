# Medical Spending Data Pipeline and Reporting Dashboard

## Overview

This project is an end-to-end data pipeline and data-driven solution enabling reporting dashboard and LLM chatbot for medical spending data. It is designed to get my hands dirty with some of the most common data engineering tools and technologies.

## Table of Contents

- [Medical Spending Data Pipeline and Reporting Dashboard](#medical-spending-data-pipeline-and-reporting-dashboard)
  - [Overview](#overview)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
  - [Project Structure](#project-structure)
  - [Project Components](#project-components)
    - [1. Digital Ocean Spaces \[S3-compatible Object Storage\]](#1-digital-ocean-spaces-s3-compatible-object-storage)
    - [2. Spark \[Compute Engine\]](#2-spark-compute-engine)
    - [3. Snowflake \[Transformation and Query Engine\]](#3-snowflake-transformation-and-query-engine)
    - [4. Dbt \[SQL Code Framework\]](#4-dbt-sql-code-framework)
    - [5. Airflow \[Orchestrator\]](#5-airflow-orchestrator)
    - [6. Docker \[Containerization\] and Docker Compose \[Container Orchestration\]](#6-docker-containerization-and-docker-compose-container-orchestration)
    - [7. Metabase \[Reporting Dashboard\]](#7-metabase-reporting-dashboard)
    - [8. Chainlit LLM Chatbot \[SQL Agent\]](#8-chainlit-llm-chatbot-sql-agent)
  - [Data Flow (orchestrated by Airflow)](#data-flow-orchestrated-by-airflow)
  - [Configuration](#configuration)
  - [Troubleshooting and FAQ](#troubleshooting-and-faq)

## Prerequisites

- Docker and Docker Compose installed
- Python 3.10+ installed
- Digital Ocean Spaces API Access (or other S3-compatible storage)
- Snowflake API Access
- Azure OpenAI API Access (for the LLM Chatbot)

## Getting Started

```
./init.sh
```

## Project Structure

```
medical-data-analytics/
├── airflow/    # Airflow project
|   ├── dags/ 
|   |   ├── dbt/ # dbt submodule
|   |   ├── .gitignore
|   |   ├── Dockerfile
|   |   ├── docker-compose.yml
|   |   ├── requirements.txt
|   ├── logs/
|   ├── plugins/
|   ├── unittests/
|   ├── .gitignore
|   ├── Dockerfile
|   ├── docker-compose.yml
|   ├── requirements.txt
├── standalone-spark/        # Spark setup (deployed as a standalone cluster)
├── chainlit/                # LLM Chatbot with SQL Agent
|   ├── chainlit_app.py      # Main application file
|   ├── .env                 # Environment variables
|   ├── .chainlit/           # Chainlit configuration
|   ├── pyproject.toml       # Project dependencies
├── README.md                # Project documentation (this file)
├── init.sh                  # Script to initialize the project
```

## Project Components

### 1. Digital Ocean Spaces [S3-compatible Object Storage]

### 2. Spark [Compute Engine]

### 3. Snowflake [Transformation and Query Engine]

### 4. Dbt [SQL Code Framework]

### 5. Airflow [Orchestrator]

### 6. Docker [Containerization] and Docker Compose [Container Orchestration]

### 7. Metabase [Reporting Dashboard]

### 8. Chainlit LLM Chatbot [SQL Agent]

The project includes a natural language interface to the Medicare spending data using a Chainlit-powered chatbot with SQL agent capabilities. This allows business users to query the Snowflake database using plain English questions instead of writing SQL.

Key features:
- Natural language to SQL translation using Azure OpenAI
- Interactive chat interface with Chainlit
- Direct querying of Snowflake data warehouse
- Explanation of SQL queries used to answer questions
- Example questions to help users get started

## Data Flow (orchestrated by Airflow)

1. Raw medicare data is extracted from CMS Open Data API and loaded into Digital Ocean Spaces (saved as json files)
   - Currently only [`medicare-part-d-spending-by-drug`](https://data.cms.gov/resources/medicare-part-d-spending-by-drug-data-dictionary) is extracted.
2. Spark is used to transform the data from json to parquet format and also to unpivot the data.
3. In Snowflake, the transformed data is loaded into as raw external tables.
4. Dbt is used to transform the data and load it into staging tables and marts.
5. The reporting dashboard is rendered in Metabase.
6. The LLM Chatbot provides a natural language interface to query the transformed data in Snowflake.

## Configuration

- In this project, `spark` and `airflow` are deployed in two separate docker compose files, and also under two separate docker networks. This is done also in goal of getting my hands dirty as I want to understand the underlying concepts of docker networks and docker compose. 
- `spark` is somehow overkill for this project, but I wanted to get my hands dirty with it.
- `dbt` and `airflow` are maintained in a separate submodule and imported as a git submodule.
- The Chainlit LLM Chatbot requires Azure OpenAI API credentials and Snowflake API credentials to be configured in the `.env` file in the chainlit directory.

## Troubleshooting and FAQ

Please refer to this [blog post](https://blog.rickyltwong.me/article/pharma-data-project#1b3f88d17ffa802d85a4d95b960b7f1e) for more details.
