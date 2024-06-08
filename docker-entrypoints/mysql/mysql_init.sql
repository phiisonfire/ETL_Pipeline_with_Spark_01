-- Create 'oltpadmin' user if it does not exist
CREATE USER IF NOT EXISTS 'oltpadmin'@'%' IDENTIFIED BY 'oltppwd';
CREATE DATABASE IF NOT EXISTS retail_db;
GRANT ALL PRIVILEGES ON retail_db.* TO 'oltpadmin'@'%';
GRANT SUPER, SYSTEM_VARIABLES_ADMIN, SESSION_VARIABLES_ADMIN ON *.* TO 'oltpadmin'@'%';

-- Create 'hiveuser' user if it does not exist
CREATE USER IF NOT EXISTS 'hiveuser'@'%' IDENTIFIED BY 'hivepassword';
CREATE DATABASE IF NOT EXISTS hive_metastore;
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hiveuser'@'%';


CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE USER IF NOT EXISTS 'airflow_user'@'%' IDENTIFIED BY 'airflow_password';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'@'%';

FLUSH PRIVILEGES;