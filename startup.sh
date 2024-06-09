#!/bin/bash

# Path to flag file
FLAG_FILE="/flag_initialized"

# Check if flag file exists
if [ ! -f "$FLAG_FILE" ]; then
    echo "Initializing Hive metastore database..."
    # Run initialization command
    hdfs dfs -mkdir       /tmp
    hdfs dfs -mkdir -p    /user/hive/warehouse
    hdfs dfs -chmod g+w   /tmp
    hdfs dfs -chmod g+w   /user/hive/warehouse

    # Initialize Hive metastore schema
    $HIVE_HOME/bin/schematool -dbType mysql -initSchema

    # Create flag file to indicate initialization
    touch "$FLAG_FILE"
fi

# Hive metastore service
echo "Starting Hive metastore service..."
$HIVE_HOME/bin/hive --service metastore &

# HiveServer2
echo "Starting HiveServer2..."
$HIVE_HOME/bin/hiveserver2 --hiveconf hive.server2.enable.doAs=false &

# Modify the .env file
sed -i 's/MYSQL_HOST=127.0.0.1/MYSQL_HOST=mysql/' /app/.env
sed -i 's/MYSQL_PORT=3307/MYSQL_PORT=3306/' /app/.env

# Change to app directory
cd /app

# Install Python dependencies
pip install .

# Update airflow.cfg with MySQL connection and load_examples setting
sed -i "s#sql_alchemy_conn = sqlite:////root/airflow/airflow.db#sql_alchemy_conn = mysql+mysqlconnector://airflow_user:airflow_password@mysql/airflow_db#" /root/airflow/airflow.cfg
sed -i "s/load_examples = True/load_examples = False/" /root/airflow/airflow.cfg
sed -i "s#dags_folder = /root/airflow/dags#dags_folder = /app/dags#" /root/airflow/airflow.cfg

# Function to check if the database is already initialized
is_db_initialized() {
  airflow db check || return 1
}

# Initialize the Airflow database if it hasn't been initialized
if is_db_initialized; then
  echo "Database is already initialized."
else
  echo "Initializing the Airflow database..."
  airflow db init
fi

# Start airflow webserver and scheduler
echo "Starting Airflow webserver at port 8080..."
airflow webserver --port 8080 &

echo "Starting Airflow scheduler..."
airflow scheduler &

/opt/spark/sbin/start-history-server.sh

tail -f /dev/null