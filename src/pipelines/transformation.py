import os
from pyspark.sql import SparkSession
import argparse
import logging

# Setup logging
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    filename=os.path.join(logs_dir, "transformation.log"),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Initialize SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("ETL Pipeline: Data Lake to DataWarehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    
    
