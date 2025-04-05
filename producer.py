from kafka import KafkaProducer
import yfinance as yf
import json
import time
import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Configure the Kafka producer with proper error handling
try:
    # Use kafka service name instead of localhost
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks='all'
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    sys.exit(1)

# List of tickers to fetch
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

def fetch_stock_data(ticker_symbol):
    """Fetch stock data with error handling"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(period="1d")
        if data.empty:
            logger.warning(f"No data returned for {ticker_symbol}")
            return None
            
        price = data['Close'].iloc[-1]
        return {
            "symbol": ticker_symbol,
            "price": round(float(price), 2),  # Ensure price is a float and rounded
            "timestamp": datetime.now().isoformat(),
            "market_timestamp": str(data.index[-1])
        }
    except Exception as e:
        logger.error(f"Error fetching data for {ticker_symbol}: {e}")
        return None

def main():
    try:
        while True:
            for ticker_symbol in tickers:
                payload = fetch_stock_data(ticker_symbol)
                if payload:
                    try:
                        future = producer.send("stock_prices", payload)
                        # Wait for the message to be delivered
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Sent: {payload} to {record_metadata.topic}, partition: {record_metadata.partition}")
                    except Exception as e:
                        logger.error(f"Failed to send message for {ticker_symbol}: {e}")
            
            logger.info("Sleeping for 10 seconds...")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.flush()  # Ensure any buffered messages are sent
        producer.close()
        sys.exit(0)

if __name__ == "__main__":
    main()
