from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Define schema for the incoming data
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType()) \
    .add("market_timestamp", StringType())


# Print debug information about environment variables
logger.info("Checking Snowflake environment variables:")
for env_var in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", 
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE"]:
    logger.info(f"{env_var} set: {os.environ.get(env_var) is not None}")

# Get Snowflake credentials from environment variables
snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT")
snowflake_user = os.environ.get("SNOWFLAKE_USER")
snowflake_password = os.environ.get("SNOWFLAKE_PASSWORD")
snowflake_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
snowflake_database = os.environ.get("SNOWFLAKE_DATABASE")
snowflake_schema = os.environ.get("SNOWFLAKE_SCHEMA")
snowflake_role = os.environ.get("SNOWFLAKE_ROLE")

def create_spark_session():
    """Create a Spark session with proper configuration"""
    try:
        # Download Snowflake JDBC driver if not already present
        spark = SparkSession.builder \
            .appName("KafkaToSnowflake") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def read_from_kafka(spark):
    """Read data from Kafka with error handling"""
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "stock_prices") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        return df
    except Exception as e:
        logger.error(f"Failed to read from Kafka: {e}")
        raise

def process_data(df):
    """Process the raw data from Kafka"""
    try:
        # Parse the JSON data
        value_df = df.selectExpr("CAST(value AS STRING)")
        
        # Convert JSON to structured data
        parsed_df = value_df.select(
            from_json(col("value"), schema).alias("data")
        ).select("data.*")
        
        # Data validation and cleaning
        validated_df = parsed_df.filter(
            (col("symbol").isNotNull()) & 
            (col("price").isNotNull()) & 
            (col("price") > 0)
        )
        
        # Add processing timestamp
        processed_df = validated_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("timestamp", to_timestamp(col("timestamp")))
        
        return processed_df
    except Exception as e:
        logger.error(f"Error during data processing: {e}")
        raise

def write_to_snowflake(df):
    """Write data to Snowflake with detailed debugging"""
    try:
        # Format Snowflake account URL
        sf_url = f"{snowflake_account}.snowflakecomputing.com"
        
        # Log connection details (without credentials)
        logger.info(f"Connecting to Snowflake at {sf_url}")
        logger.info(f"Using database: {snowflake_database}, schema: {snowflake_schema}, warehouse: {snowflake_warehouse}")
        logger.info(f"Role: {snowflake_role}")
        
        # Show what we're trying to write (sample data)
        logger.info("Sample data to be written:")
        df.show(5, truncate=False)
        
        schema = snowflake_schema if snowflake_schema else "PUBLIC"
        
        # Additional options for debugging
        options = {
            "sfURL": sf_url,
            "sfUser": snowflake_user,
            "sfPassword": snowflake_password,
            "sfDatabase": snowflake_database,
            "sfSchema": schema,
            "sfWarehouse": snowflake_warehouse,
            "sfRole": snowflake_role,
            "dbtable": "STOCK_PRICES",
            "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
            "truncate_table": "off",
            "autopushdown": "off",
            "jdbc.use.legacy.datetime.code": "false",
            "purge": "off"
        }
        
        # Log all options (except password)
        safe_options = {k: v for k, v in options.items() if k != "sfPassword"}
        logger.info(f"Snowflake connection options: {safe_options}")
        
        # Attempt to write with detailed error capturing
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**options) \
            .mode("append") \
            .save()
        
        logger.info("Data successfully written to Snowflake")
    except Exception as e:
        logger.error(f"Failed to write to Snowflake. Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        
        # Print the full stack trace for debugging
        import traceback
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())


def main():
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Read from Kafka
        kafka_df = read_from_kafka(spark)
        
        # Process data
        processed_df = process_data(kafka_df)
        
        # Show the processed data
        logger.info("Processed data schema:")
        processed_df.printSchema()
        logger.info("Sample processed data:")
        processed_df.show(10)
        
        # Write to Snowflake
        write_to_snowflake(processed_df)
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
    finally:
        # Ensure clean shutdown
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
