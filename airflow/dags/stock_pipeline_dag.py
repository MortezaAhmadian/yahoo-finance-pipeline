from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os

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

# Start Kafka producer
start_kafka_producer = BashOperator(
    task_id='start_kafka_producer',
    bash_command='python /opt/airflow/dags/producer.py &',  # Run in background
    dag=dag
)

run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command="""
    # Load variables from .env file
    PROJECT_ROOT='/opt/airflow'
    if [ -f "$PROJECT_ROOT/.env" ]; then
        # Read the .env file line by line
        while IFS='=' read -r key value || [ -n "$key" ]; do
            # Skip comments and empty lines
            [[ $key == \#* ]] && continue
            [[ -z "$key" ]] && continue
            # Remove quotes if present
            value=$(echo $value | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
            # Set the environment variable
            export $key="$value"
            echo "Loaded $key from .env file"
        done < "$PROJECT_ROOT/.env"
    else
        echo "Warning: .env file not found at $PROJECT_ROOT/.env"
        exit 1
    fi
    
    # Use hardcoded container name instead of docker format command
    CONTAINER_NAME="realtime_data_platform-spark-worker-1"
    
    echo "Using container: $CONTAINER_NAME"
    echo "Using Snowflake Account: $SNOWFLAKE_ACCOUNT"
    
    # Pass environment variables to the Spark job
    docker exec \\
      -e SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \\
      -e SNOWFLAKE_USER="$SNOWFLAKE_USER" \\
      -e SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \\
      -e SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \\
      -e SNOWFLAKE_DATABASE="$SNOWFLAKE_DATABASE" \\
      -e SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMA" \\
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
services_check >> create_snowflake_table >> start_kafka_producer >> run_spark_job >> data_validation
