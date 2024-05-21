import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

load_dotenv()

mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_database_name = os.getenv('MYSQL_DATABASE')

# create spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Ingestion - from OLTP Database (MySQL) to DataLake (Hadoop HDFS)") \
    .getOrCreate()

# get the latest records from mysql
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://" + mysql_host + ":3306/" + mysql_database_name) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "Customer") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .load()

table_location = "hdfs://localhost:9900/datalake/Customer"

output_df = df.withColumn("year", lit(2024)).withColumn("month", lit(5)).withColumn("day", lit(21))
output_df.write.partitionBy("year", "month", "day").mode("append").parquet(table_location)