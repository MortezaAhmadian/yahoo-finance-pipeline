# Yahoo Finance Real-Time Data Pipeline

A comprehensive data engineering solution that streams real-time stock market data from Yahoo Finance to Snowflake for analytics.

## Architecture Overview

This project implements a complete data pipeline with the following components:

- **Data Source**: Yahoo Finance API for real-time stock price data
- **Ingestion**: Kafka producer to stream stock price data
- **Processing**: Apache Spark for data transformation and enrichment
- **Storage**: Snowflake for data warehousing
- **Orchestration**: Apache Airflow for workflow management
- **Containerization**: Docker for consistent deployment across environments

![Architecture Diagram](https://via.placeholder.com/800x400?text=Yahoo+Finance+Pipeline+Architecture)

## Key Features

- Real-time streaming of stock price data using Kafka
- Scalable data processing with Apache Spark
- Cloud-based data warehousing with Snowflake
- Automated workflow management with Airflow
- Containerized deployment for easy setup and scalability
- End-to-end pipeline monitoring

## Prerequisites

- Docker and Docker Compose
- Snowflake account with appropriate permissions
- Internet connection for Yahoo Finance API access

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/yahoo-finance-pipeline.git
   cd yahoo-finance-pipeline
   ```

2. Create a `.env` file with your Snowflake credentials:
   ```
   SNOWFLAKE_ACCOUNT=your_account_identifier
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=STOCK_DATA_WH
   SNOWFLAKE_DATABASE=STOCK_DATA
   SNOWFLAKE_SCHEMA=PUBLIC
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   ```

3. Start the services:
   ```bash
   docker-compose up -d
   ```

4. Access Airflow web UI:
   ```
   http://localhost:8082
   ```
   Login with username `admin` and password `admin`

5. Trigger the `stock_pipeline_dag` to start the pipeline

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── stock_pipeline_dag.py    # Airflow DAG definition
│   │   └── producer.py              # Kafka producer for Yahoo Finance data
│   ├── Dockerfile                   # Airflow container configuration
│   └── requirements.txt             # Python dependencies for Airflow
├── spark_jobs/
│   └── spark_job.py                 # Spark processing application
├── docker-compose.yml               # Defines all services
├── .env                             # Configuration variables (not in Git)
└── README.md                        # Project documentation
```

## Component Details

### Kafka Producer (producer.py)
Fetches real-time stock price data from Yahoo Finance and publishes it to a Kafka topic.

### Spark Processing (spark_job.py)
Consumes data from Kafka, performs transformations, and writes to Snowflake for storage and analysis.

### Airflow DAG (stock_pipeline_dag.py)
Orchestrates the entire pipeline, managing dependencies between tasks and ensuring successful execution.

## Data Flow

1. Airflow triggers the Kafka producer to fetch stock data from Yahoo Finance
2. Stock data is published to the Kafka topic "stock_prices"
3. Spark application consumes data from Kafka and performs transformations
4. Processed data is written to Snowflake for storage and analytics
5. Airflow validates the data has been successfully processed

## Development and Customization

### Adding New Stocks
Modify the `tickers` list in `producer.py` to include additional stock symbols.

### Custom Transformations
Extend the Spark processing logic in `spark_job.py` to implement additional data transformations.

### Scheduling
Adjust the `schedule_interval` parameter in the Airflow DAG to change how frequently the pipeline runs.

## Troubleshooting

### Common Issues

1. **Snowflake Connection Issues**
   - Verify credentials in `.env` file
   - Ensure proper network connectivity to Snowflake

2. **Spark Job Failures**
   - Check logs with `docker-compose logs spark-worker`
   - Verify packages are correctly specified in the spark-submit command

3. **Kafka Connectivity Problems**
   - Ensure Kafka and Zookeeper are running: `docker-compose ps`
   - Check Kafka topics: `docker-compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092`

## Contributors

- Morteza Ahmadian

## Acknowledgments

- Yahoo Finance for providing the API
- The Apache community for Kafka, Spark, and Airflow
- Snowflake for their data warehouse solution
