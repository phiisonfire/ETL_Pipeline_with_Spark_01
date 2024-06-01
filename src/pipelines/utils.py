from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

# Add ANSI escape sequences for bold yellow text
BOLD_YELLOW = "\033[1m\033[93m"
RESET = "\033[0m"

def print_bold(string: str) -> None:
    print(f"{BOLD_YELLOW}{string}{RESET}")

def get_hdfs_FileSystem_obj(spark: SparkSession, hdfs_uri: str) -> bool:
    """
    Creates and returns a Hadoop FileSystem object for accessing HDFS.

    Parameters:
    spark (SparkSession): The active SparkSession.
    hdfs_uri (str): The URI of the HDFS namenode.

    Returns:
    FileSystem: The Hadoop FileSystem object configured with the given HDFS URI.

    Usage:
    fs = get_hdfs_FileSystem_obj(spark, "hdfs://localhost:9000")
    fs.exist(hdfs_path_object)
    """
    
    # Import necessary classes from Java
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileStatus')
    
    # Get the Hadoop configuration
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.defaultFS", hdfs_uri)

    # Create a FileSystem object
    return spark._jvm.FileSystem.get(hadoop_conf)

def get_hdfs_path_object(spark: SparkSession, hdfs_path: str):
    """
    This function receive a hdfs_path as string and return a jvm.Path() object that can be used with Hadoop FileSystem Object.
    """
    return spark._jvm.Path(hdfs_path)