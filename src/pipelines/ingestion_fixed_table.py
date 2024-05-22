import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
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
    table_names = ["Product", "ProductCategory"]
    
    if table_name not in table_names:
        print(f"{BOLD_YELLOW}Table name '{table_name}' is not recognized.{RESET}")
        logger.error(f"Table name '{table_name}' is not recognized.")
        return
    
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
        .master("local") \
        .appName("Ingestion fixed tables - from OLTP Database (MySQL) to DataLake (Hadoop HDFS)") \
        .getOrCreate()
    
    try:
        fs = get_hdfs_FileSystem_obj(spark, hdfs_uri="hdfs://localhost:9900")
        table_dir_str = "/datalake/" + table_name
        hdfs_table_dir_path = spark._jvm.Path(table_dir_str)
        
        # Check for existing data in HDFS
        if fs.exists(hdfs_table_dir_path):
            print(f"{BOLD_YELLOW}Table {table_name} is already ingested into HDFS.{RESET}")
            logger.error(f"Table {table_name} is already ingested into HDFS.")
            return
        
        # Load new data from MySQL
        mysql_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:mysql://{mysql_host}:3306/{mysql_database_name}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .load()
            
        mysql_df.write.parquet(table_dir_str)
        print(f"{BOLD_YELLOW}Ingested {table_name} into in datalake.{RESET}")
        logger.info(f"{BOLD_YELLOW}Ingested {table_name} into in datalake.{RESET}")
    
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