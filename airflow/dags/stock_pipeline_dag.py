from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os
import importlib.util


default_args = {
    'owner': 'data_team',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False
}

dag = DAG(
    'stock_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['stocks', 'finance', 'data-pipeline']
)

# Health check task before starting the pipeline
services_check = BashOperator(
    task_id='services_check',
    bash_command="""
    nc -z kafka 9092 && \
    nc -z spark-master 7077 && \
    echo "All services are up and running"
    """,
    dag=dag
)

# Create Snowflake table if it doesn't exist
create_snowflake_table = SnowflakeOperator(
    task_id='create_snowflake_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS STOCK_PRICES (
        symbol STRING NOT NULL,
        price FLOAT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        market_timestamp STRING,
        processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    dag=dag
)

def run_producer():
    spec = importlib.util.spec_from_file_location("producer", "/opt/producer.py")
    producer_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(producer_module)

    # Start producer and let it run
    producer_module.start_producer()

# start spark job
run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command="""
    # The .env file should already be in place thanks to our entrypoint script
    ENV_FILE="/opt/airflow/.env"
    if [ -f "$ENV_FILE" ]; then
        echo "Found .env file at $ENV_FILE"
    else
        echo "Warning: .env file not found at $ENV_FILE, will use environment variables"
    fi
    
    # Use hardcoded container name
    CONTAINER_NAME="realtime_data_platform-spark-worker-1"
    echo "Using container: $CONTAINER_NAME"
    
    # Pass environment variables to the Spark job
    docker exec \\
      -e SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \\
      -e SNOWFLAKE_USER="$SNOWFLAKE_USER" \\
      -e SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \\
      -e SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \\
      -e SNOWFLAKE_DATABASE="$SNOWFLAKE_DATABASE" \\
      -e SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMoA" \\
      -e SNOWFLAKE_ROLE="$SNOWFLAKE_ROLE" \\
      $CONTAINER_NAME /opt/bitnami/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 \\
      --conf "spark.executor.memory=2g" \\
      --conf "spark.driver.memory=1g" \\
      /opt/spark-apps/spark_job.py
    """,
    dag=dag
)

# Check if data was processed successfully in Snowflake
data_validation = SnowflakeOperator(
    task_id='data_validation',
    snowflake_conn_id='snowflake_default',
    sql="""
    SELECT COUNT(*) AS record_count 
    FROM STOCK_PRICES 
    WHERE processing_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP());
    """,
    do_xcom_push=True,
    dag=dag
)

# Set task dependencies
services_check >> create_snowflake_table >> run_spark_job >> data_validation
