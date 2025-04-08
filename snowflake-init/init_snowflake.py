#!/usr/bin/env python3
import snowflake.connector
import os
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Get Snowflake credentials from environment variables
snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT")
snowflake_user = os.environ.get("SNOWFLAKE_USER")
snowflake_password = os.environ.get("SNOWFLAKE_PASSWORD")
snowflake_role = os.environ.get("INITIAL_ROLE", "ACCOUNTADMIN")  # Use ACCOUNTADMIN initially

# Target resources to create
target_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
target_database = os.environ.get("SNOWFLAKE_DATABASE")
target_schema = os.environ.get("SNOWFLAKE_SCHEMA")
target_role = os.environ.get("SNOWFLAKE_ROLE")

def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

def wait_for_snowflake():
    """Wait for Snowflake to become available"""
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Try connecting to Snowflake
            conn = snowflake.connector.connect(
                user=snowflake_user,
                password=snowflake_password,
                account=snowflake_account,
                role=snowflake_role
            )
            conn.close()
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            retry_count += 1
            logger.warning(f"Snowflake not available yet (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(10)  # Wait 10 seconds before retrying
    
    logger.error("Failed to connect to Snowflake after multiple attempts")
    return False

def setup_snowflake():
    """Set up all required Snowflake resources"""
    try:
        # Connect with ACCOUNTADMIN role
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            role=snowflake_role
        )
        
        # Create warehouse if it doesn't exist
        warehouse_query = f"""
        CREATE WAREHOUSE IF NOT EXISTS {target_warehouse}
          WITH WAREHOUSE_SIZE = 'XSMALL'
          AUTO_SUSPEND = 60
          AUTO_RESUME = TRUE;
        """
        execute_query(conn, warehouse_query)
        logger.info(f"Warehouse {target_warehouse} created or already exists")
        
        # Create database if it doesn't exist
        database_query = f"CREATE DATABASE IF NOT EXISTS {target_database};"
        execute_query(conn, database_query)
        logger.info(f"Database {target_database} created or already exists")
        
        # Use the database
        use_db_query = f"USE DATABASE {target_database};"
        execute_query(conn, use_db_query)
        
        # Create schema if it doesn't exist
        schema_query = f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
        execute_query(conn, schema_query)
        logger.info(f"Schema {target_schema} created or already exists")
        
        # Create role if it doesn't exist
        role_exists_query = f"SHOW ROLES LIKE '{target_role}';"
        roles = execute_query(conn, role_exists_query)
        
        if not roles:
            # Create the role
            create_role_query = f"CREATE ROLE {target_role};"
            execute_query(conn, create_role_query)
            logger.info(f"Role {target_role} created")
        else:
            logger.info(f"Role {target_role} already exists")
        
        # Grant privileges to the role
        grant_queries = [
            f"GRANT USAGE ON DATABASE {target_database} TO ROLE {target_role};",
            f"GRANT USAGE ON SCHEMA {target_database}.{target_schema} TO ROLE {target_role};",
            f"GRANT CREATE TABLE ON SCHEMA {target_database}.{target_schema} TO ROLE {target_role};",
            f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {target_database}.{target_schema} TO ROLE {target_role};",
            f"GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA {target_database}.{target_schema} TO ROLE {target_role};",
            f"GRANT USAGE ON WAREHOUSE {target_warehouse} TO ROLE {target_role};"
        ]
        
        for query in grant_queries:
            execute_query(conn, query)
        logger.info(f"Privileges granted to role {target_role}")
        
        # Grant role to user
        grant_role_query = f"GRANT ROLE {target_role} TO USER {snowflake_user};"
        execute_query(conn, grant_role_query)
        logger.info(f"Role {target_role} granted to user {snowflake_user}")
        
        # Create the stock prices table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_database}.{target_schema}.STOCK_PRICES (
            symbol STRING NOT NULL,
            price FLOAT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            market_timestamp STRING,
            processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """
        execute_query(conn, create_table_query)
        logger.info("STOCK_PRICES table created or already exists")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error setting up Snowflake resources: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting Snowflake initialization script")
    
    if wait_for_snowflake():
        if setup_snowflake():
            logger.info("Snowflake setup completed successfully")
        else:
            logger.error("Snowflake setup failed")
            exit(1)
    else:
        logger.error("Could not connect to Snowflake")
        exit(1)
