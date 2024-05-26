import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import argparse
import logging

# Setup logging
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    filename=os.path.join(logs_dir, "ingestion.log"),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Add ANSI escape sequences for bold yellow text
BOLD_YELLOW = "\033[1m\033[93m"
RESET = "\033[0m"

def get_hdfs_FileSystem_obj(spark: SparkSession, hdfs_uri: str):
    from py4j.java_gateway import java_import
    
    # Import necessary classes from Java
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileStatus')
    
    # Get the Hadoop configuration
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.defaultFS", hdfs_uri)

    # Create a FileSystem object
    return spark._jvm.FileSystem.get(hadoop_conf)

def main(table_name: str) -> None:
    # Define table-specific details
    table_details = {
        "Customer": {"primary_col": "CustomerID", "date_col": "ModifiedDate"},
        "SalesOrderHeader": {"primary_col": "SalesOrderID", "date_col": "OrderDate"},
        "SalesOrderDetail": {"primary_col": "SalesOrderID", "date_col": "ModifiedDate"}
    }
    
    if table_name not in table_details:
        print(f"{BOLD_YELLOW}Table name '{table_name}' is not recognized.{RESET}")
        logger.error(f"Table name '{table_name}' is not recognized.")
        return
    
    primary_col = table_details[table_name]["primary_col"]
    date_col = table_details[table_name]["date_col"]
    
    # Load environment variables
    load_dotenv()
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_database_name = os.getenv('MYSQL_DATABASE')
    
    if not mysql_user or not mysql_password or not mysql_host or not mysql_database_name:
        print(f"{BOLD_YELLOW}Missing one or more required environment variables.{RESET}")
        logger.error("Missing one or more required environment variables.")
        return

    # Create Spark session
    spark = SparkSession.builder \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores", "1") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "2") \
        .appName(f"Ingesting {table_name} - from OLTP Database (MySQL) to DataLake (Hadoop HDFS)") \
        .getOrCreate()
    
    try:
        fs = get_hdfs_FileSystem_obj(spark, hdfs_uri="hdfs://localhost:9000")
        table_dir_str = "/datalake/" + table_name
        hdfs_table_dir_path = spark._jvm.Path(table_dir_str)
        
        # Check for existing data in HDFS
        if fs.exists(hdfs_table_dir_path):
            hdfs_df = spark.read.parquet(table_dir_str)
            hdfs_df.createOrReplaceTempView("hdfs_table")
            latest_record = spark.sql(f"SELECT MAX({primary_col}) FROM hdfs_table").collect()[0][0]
        else:
            latest_record = 0
        
        # Load new data from MySQL
        new_records_from_mysql_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:mysql://{mysql_host}:3306/{mysql_database_name}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("partitionColumn", primary_col) \
            .option("lowerBound", latest_record + 1) \
            .option("upperBound", sys.maxsize) \
            .option("numPartitions", 2) \
            .load()
        
        # Persist DataFrame both in memory and on disk for multiple reusage
        new_records_from_mysql_df.persist(StorageLevel.MEMORY_AND_DISK)
        
        new_records_cnt = new_records_from_mysql_df.count()
        if new_records_cnt:
            print(f"{BOLD_YELLOW}Ingesting {new_records_cnt} new records into table {table_name} in datalake.{RESET}")
            logger.info(f"Ingesting {new_records_cnt} new records into table {table_name} in datalake.")
            new_records_from_mysql_df.write.partitionBy("year", "month", "day").mode("append").parquet(table_dir_str)
        else:
            print(f"{BOLD_YELLOW}Table {table_name} in Datalake is up to date with MySQL Database.{RESET}")
            logger.info(f"Table {table_name} in Datalake is up to date with MySQL Database.")
    
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Ingest data from MySQL to HDFS")
    parser.add_argument("--table_name", "-t", required=True, type=str, help="Name of the table to ingest")
    args = parser.parse_args()

    # Call main function with the provided table_name argument
    main(args.table_name)