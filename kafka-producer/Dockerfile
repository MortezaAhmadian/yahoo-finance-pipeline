FROM python:3.8-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir yfinance kafka-python

# Copy the producer script
COPY producer.py /app/

# Run the producer
CMD ["python", "producer.py"]
