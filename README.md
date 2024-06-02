# ETL_Pipeline_with_Spark_01
## bash usages
1. create database schema for mysql

python scripts/02_create_retail_db_schema.py \
--database_schema_path /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/sample_data/retail_db_schema.yaml

2. load table Product, ProductCategory from .csv files into corresponding tables in mysql

python scripts/01_load_database_table_from_csv.py --csv_table_file_path /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/sample_data/ProductCategory.csv --csv_table_schema_file_path /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/sample_data/ProductCategory.yaml --table_name ProductCategory

python scripts/01_load_database_table_from_csv.py --csv_table_file_path /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/sample_data/Product.csv --csv_table_schema_file_path /home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/sample_data/Product.yaml --table_name Product

3. generating new (fake) data
python scripts/07_generate_multiple_date_sales_data_with_load.py \
-n 1000000 \
-sdate 2024-02-06 \
-edate 2024-02-10

4. ingest fixed tables (ProductCategory & Product), without partitioning
./spark-submit \
--master yarn \
--jars /home/phinguyen/lib/mysql-connector-j-8.0.33.jar \
/home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/src/pipelines/ingestion_fixed_table.py \
--table_name ProductCategory

5. ingest Customers, SalesOrderHeader and SalesOrderDetail tables
./spark-submit \
--master yarn \
--jars /home/phinguyen/lib/mysql-connector-j-8.0.33.jar \
/home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/src/pipelines/ingestion.py \
--table_name Customer

6. create schema in Hive
CREATE TABLE ProductCategory (
  ProductCategoryKey INT PRIMARY KEY UNIQUE,
  EnglishProductCategoryName VARCHAR(255) UNIQUE
);

7. ETL datalake -> Hive

./spark-submit \
--master yarn \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/home/phinguyen/data_engineering/ETL_Pipeline_with_Spark_01/src/pipelines/transformation.py
