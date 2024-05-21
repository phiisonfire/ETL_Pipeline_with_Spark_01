import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

load_dotenv()

mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_database_name = os.getenv('MYSQL_DATABASE')

conf = SparkConf()  # create the configuration
conf.set("spark.jars", "/home/phinguyen/lib/mysql-connector-j-8.0.33.jar") 

spark = SparkSession.builder \
    .config(conf=conf) \
    .master("local") \
    .appName("MySQLConnection") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://" + mysql_host + ":3306/" + mysql_database_name) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "Customer") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .load()

df.show()