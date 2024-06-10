# Data Engineering Project | Synthetic Retail Data
![architecture](https://github.com/phiisonfire/ETL_Pipeline_with_Spark_01/assets/87942974/c2ccf8ee-4f6d-456b-9aa2-f4c40415c598)


## Overview

This project consists of two main pipelines for data processing and management using a combination of **MySQL, Hadoop HDFS, Apache Hive, Apache Spark, and Apache Airflow**. The pipelines are designed to incrementally ingest data from an OLTP database into a data lake and transform the data for storage in a data warehouse. All services run on **Docker containers**.

## Pipelines

### 1. Ingestion Pipeline

The ingestion pipeline is responsible for extracting data from an OLTP MySQL database and saving it to a data lake in HDFS. The data is stored in Parquet format, partitioned by `year`, `month`, and `day`. There is no transformation of data during this process.

**Key Components:**
- **Source:** OLTP Database (MySQL)
- **Destination:** Data Lake (Hadoop HDFS)
- **Format:** Parquet
- **Partitioning:** `year`, `month`, `day`
- **Tools:** Apache Spark, Apache Airflow

### 2. Transformation Pipeline

The transformation pipeline extracts data from the data lake, applies transformations to fit a new schema, and saves the transformed data to a data warehouse (Hive). The data warehouse runs on top of the same Hadoop HDFS as the data lake.

**Key Components:**
- **Source:** Data Lake (Hadoop HDFS)
- **Destination:** Data Warehouse (Hive on HDFS)
- **Tools:** Apache Spark, Apache Airflow

## Technology Stack

- **OLTP Database:** MySQL
- **Data Lake:** Hadoop HDFS
- **Data Warehouse:** Hive (on HDFS)
- **ETL Engine:** Apache Spark (running on YARN)
- **Orchestration Tool:** Apache Airflow

## Project Structure

The project directory is structured as follows:
```bash
.
├── configs.toml
├── dags # Airflow DAGs
├── docker-compose.yaml # start up all services
├── Dockerfile # Dockerfile for service which is serving as client for Spark, Hive and Airflow
├── docker-hadoop-base
├── entrypoint.sh
├── hadoop.env # configuration of Hadoop, Hive & Spark cluster
├── hive_conf
├── mysql_init.sql
├── notebooks # jupyter notebook experiments
├── README.md
├── requirements.txt
├── sample_data # example data for Retail OLTP Database
├── scripts
├── setup.py
├── src
│   ├── exception.py
│   ├── generator # module for generating synthetic data
│   │   ├── generator.py
│   │   ├── __init__.py
│   │   ├── load_table_from_csv.py
│   │   └── schema.py
│   ├── __init__.py
│   ├── logger.py
│   ├── pipelines
│   │   ├── ingestion_fixed_table.py
│   │   ├── ingestion.py
│   │   ├── __init__.py
│   │   ├── transformation.py
│   │   └── utils.py
│   └── utils
│       ├── db_connection.py # helper module for connecting with MySQL Database
│       └── __init__.py
└── startup.sh
```
## Using Docker Services from local host
```bash
localhost:18080 # Spark History Server UI
localhost:8080 # Airflow Webserver
localhost:8088 # Yarn resource manager
localhost:9870 # Hadoop Namenode
```

## Project reproduce
```bash
$ docker build -t bde2020/hadoop-base:2.0.0-hadoop3.3.6-java8-ubuntu22.04 ./docker-hadoop-base
$ docker compose up --build
$ docker exec -it client /bin/bash # login to client container
# below commands should be executed inside docker client container
$ cd /app
$ python3 scripts/01_create_retail_db_schema.py # create schema for retail database in mysql

# load data from csv for tables ProductCategory and Product
$ python3 scripts/02_load_database_table_from_csv.py \
--csv_table_file_path /app/sample_data/ProductCategory.csv \
--csv_table_schema_file_path /app/sample_data/ProductCategory.yaml \
--table_name ProductCategory

$ python3 scripts/02_load_database_table_from_csv.py \
--csv_table_file_path /app/sample_data/Product.csv \
--csv_table_schema_file_path /app/sample_data/Product.yaml \
--table_name Product

# generate 1 millions sale orders each day from 2024-01-01 to 2024-02-01
$ python3 scripts/03_generate_multiple_date_sales_data_with_load.py \
-n 1000000 \
-sdate 2024-02-15 \
-edate 2024-03-15

# ingest fixed tables ProductCategory and Product from OLTP Database to Data Lake
$ /opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
/app/src/pipelines/ingestion_fixed_table.py \
--table_name ProductCategory

$ /opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
/app/src/pipelines/ingestion_fixed_table.py \
--table_name Product

# ingest changing tables Customers, SalesOrderHeader and SalesOrderDetail from OLTP Database to Data Lake
# this is incrementally ingesting, run everyday as batch night job
/opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/app/src/pipelines/ingestion.py \
--table_name Customer

/opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/app/src/pipelines/ingestion.py \
--table_name SalesOrderHeader

/opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/app/src/pipelines/ingestion.py \
--table_name SalesOrderDetail

# ETL pipeline from Data Lake to Data Warehouse
# This pipeline extracts data from Data Lake as OLTP format, transforms it into OLAP format (start-schema) then load it into Data Warehouse
/opt/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/app/src/pipelines/transformation.py
```
- The incrementally ingesting pipeline from OLTP Database to Data Lake and transformation pipeline from Data Lake to Data Warehouse are automatically executed and managed by Airflow. Airflow webserver is exposed to localhost via port `localhost:8080`.
