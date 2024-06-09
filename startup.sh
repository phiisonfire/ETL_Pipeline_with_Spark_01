#!/bin/bash

# Path to hive flag file
HIVE_FLAG_FILE="/hive_flag_initialized"

# Check if flag file exists
if [ ! -f "$HIVE_FLAG_FILE" ]; then
    echo "Initializing Hive metastore database..."
    # Run initialization command
    hdfs dfs -mkdir       /tmp
    hdfs dfs -mkdir -p    /user/hive/warehouse
    hdfs dfs -chmod g+w   /tmp
    hdfs dfs -chmod g+w   /user/hive/warehouse

    # Initialize Hive metastore schema
    $HIVE_HOME/bin/schematool -dbType mysql -initSchema

    # Create flag file to indicate initialization
    touch "$HIVE_FLAG_FILE"
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

# Path to hive flag file
AIRFLOW_FLAG_FILE="/airflow_flag_initialized"

# Check if flag file exists
if [ ! -f "$AIRFLOW_FLAG_FILE" ]; then
    # Initialize Hive airflow backend with defaults
    airflow db init

    # Change airflow configurations
    sed -i "s#sql_alchemy_conn = .*#sql_alchemy_conn = mysql+mysqlconnector://airflow_user:airflow_password@mysql/airflow_db#" /root/airflow/airflow.cfg
    sed -i "s#dags_folder = .*#dags_folder = /app/dags#" /root/airflow/airflow.cfg
    sed -i "s/load_examples = True/load_examples = False/" /root/airflow/airflow.cfg

    # re-initialize the backend database
    airflow db init

    # create user
    airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

    # Create flag file to indicate initialization
    touch "$AIRFLOW_FLAG_FILE"
fi

# Start airflow webserver and scheduler
echo "Starting Airflow webserver at port 8080..."
airflow webserver --port 8080 &

echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Starting Airflow scheduler at port 18080..."
/opt/spark/sbin/start-history-server.sh

tail -f /dev/null