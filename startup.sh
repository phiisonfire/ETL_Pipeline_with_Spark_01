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

# Paths
APP_DIR="/app"
SOURCE_DIR="/source"

# Create necessary directories
mkdir -p $APP_DIR/dags
mkdir -p $APP_DIR/sample_data
mkdir -p $APP_DIR/scripts
mkdir -p $APP_DIR/src

# Copy files from source to app directory
cp -r $SOURCE_DIR/dags/* $APP_DIR/dags/
cp -r $SOURCE_DIR/sample_data/* $APP_DIR/sample_data/
cp -r $SOURCE_DIR/scripts/* $APP_DIR/scripts/
cp -r $SOURCE_DIR/src/* $APP_DIR/src/
cp $SOURCE_DIR/__init__.py $APP_DIR/
cp $SOURCE_DIR/requirements.txt $APP_DIR/
cp $SOURCE_DIR/setup.py $APP_DIR/
cp $SOURCE_DIR/.env $APP_DIR/

# Modify the .env file
sed -i 's/MYSQL_HOST=127.0.0.1/MYSQL_HOST=mysql/' $APP_DIR/.env
sed -i 's/MYSQL_PORT=3307/MYSQL_PORT=3306/' $APP_DIR/.env

# Change to app directory
cd $APP_DIR

# Install Python dependencies
pip install .

/opt/spark/sbin/start-history-server.sh

tail -f /dev/null