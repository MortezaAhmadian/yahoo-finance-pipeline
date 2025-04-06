#!/bin/bash
set -e

# Generate .env file from environment variables
echo "Generating .env file from environment variables..."
cat > /opt/airflow/.env << EOF
SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}
SNOWFLAKE_USER=${SNOWFLAKE_USER}
SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE}
SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE}
SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA}
SNOWFLAKE_ROLE=${SNOWFLAKE_ROLE}
EOF

chmod 600 /opt/airflow/.env
echo ".env file created at /opt/airflow/.env"

# Execute the CMD
exec "$@"
