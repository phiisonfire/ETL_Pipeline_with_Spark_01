from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_pipelines',
    default_args=default_args,
    description='Ingestion pipeline - incrementally loading from OLTP Database (MySQL) into Data Lake (HDFS)',
    schedule_interval=timedelta(days=1),
    tags=['ETL_dags']
)

submit_spark_ingestion_job_template = '/home/phinguyen/data_engineering/spark-3.5.1-bin-hadoop3/bin/spark-submit \
        --master yarn \
        --jars /home/phinguyen/lib/mysql-connector-j-8.0.33.jar \
        --driver-cores 1 \
        --driver-memory 1g \
        --executor-cores 2 \
        --executor-memory 2g \
        /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/src/pipelines/ingestion.py \
        --table_name {}'

# tasks
ingesting_customer_table = BashOperator(
    task_id='ingesting_customer_table',
    bash_command=submit_spark_ingestion_job_template.format('Customer'),
    dag=dag
)

ingesting_salesOrderHeader_table = BashOperator(
    task_id='ingesting_salesOrderHeader_table',
    bash_command=submit_spark_ingestion_job_template.format('SalesOrderHeader'),
    dag=dag
)

ingesting_salesOrderDetail_table = BashOperator(
    task_id='ingesting_salesOrderDetail_table',
    bash_command=submit_spark_ingestion_job_template.format('SalesOrderDetail'),
    dag=dag
)

etl_lake_to_warehouse = BashOperator(
    task_id='etl_lake_to_warehouse',
    bash_command='/home/phinguyen/data_engineering/spark-3.5.1-bin-hadoop3/bin/spark-submit \
        --master yarn \
        --driver-cores 1 \
        --driver-memory 1g \
        --executor-cores 2 \
        --executor-memory 2g \
        /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/src/pipelines/transformation.py',
    dag=dag
)

# Set task dependencies
ingesting_customer_table >> ingesting_salesOrderHeader_table >> ingesting_salesOrderDetail_table >> etl_lake_to_warehouse