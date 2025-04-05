FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir snowflake-connector-python

# Copy the initialization script
COPY init_snowflake.py /app/

# Make the script executable
RUN chmod +x /app/init_snowflake.py

# Run the script
CMD ["python", "/app/init_snowflake.py"]
