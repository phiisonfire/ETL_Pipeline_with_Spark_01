import os
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col

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
        .appName("ETL Pipeline: Data Lake to Data Warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    lake_Product_df = spark.read.parquet("hdfs://localhost:9000/datalake/Product")
    lake_Customer_df = spark.read.parquet('hdfs://localhost:9000/datalake/Customer')
    lake_SalesOrderHeader_df = spark.read.parquet("hdfs://localhost:9000/datalake/SalesOrderHeader")
    lake_SalesOrderDetail_df = spark.read.parquet("hdfs://localhost:9000/datalake/SalesOrderDetail")
    
    lake_SalesOrderDetail_df.createOrReplaceTempView('SalesOrderDetail')
    lake_SalesOrderHeader_df.createOrReplaceTempView('SalesOrderHeader')
    lake_Product_df.createOrReplaceTempView('Product')
    
    FactSales_df = spark.sql("""
        SELECT
            d.SalesOrderID
            , SalesOrderLineNumber
            , OrderDate
            , CustomerID
            , d.ProductKey
            , Qty AS Quantity
            , StandardCost
            , ListPrice
            , Quantity * ListPrice AS LineAmount
        FROM SalesOrderDetail d
        LEFT JOIN SalesOrderHeader h ON d.SalesOrderID = h.SalesOrderID
        LEFT JOIN Product p ON d.Productkey = p.Productkey
        ORDER BY SalesOrderID, SalesOrderLineNumber
    """)
    
    if not spark.catalog.tableExists('dimcustomer'):
        lake_Customer_df.write.mode('overwrite').saveAsTable('dimcustomer')
        FactSales_df.write.mode('overwrite').partitionBy('OrderDate').saveAsTable('factsales')
    else:
        maxCustomerIdInHive = spark.sql("SELECT MAX(CustomerId) from dimcustomer").collect()[0][0]
        maxSalesOrderIdInHive = spark.sql("SELECT MAX(SalesOrderId) from factsales").collect()[0][0]
        
        lake_Customer_df.filter(col('CustomerId') > maxCustomerIdInHive)\
            .write.mode('append').saveAsTable('dimcustomer')
        
        FactSales_df.filter(col('SalesOrderId') > maxSalesOrderIdInHive)\
            .write.mode('append').partitionBy('OrderDate').saveAsTable('factsales')

if __name__ == '__main__':
    main()
        
    
    
    
