import pika 
import json
import csv
import time
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = "fraud_detection_queue"

# Sample data CSV file
SAMPLE_DATA_CSV = "../data/new_applications.csv"

def create_queue(channel, queue_name):
    """Create queue if not exists"""
    try:
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Queue '{queue_name}' created")
    except Exception as e:
        logger.error(f"Error creating queue {queue_name}: {e}")

def send_sample_transactions():
    """Send sample transactions from CSV to RabbitMQ"""
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        # Create queue
        create_queue(channel, RABBITMQ_QUEUE)
        
        # Read and send CSV data
        if os.path.exists(SAMPLE_DATA_CSV):
            df = pd.read_csv(SAMPLE_DATA_CSV)
            logger.info(f"Loaded {len(df)} transactions from {SAMPLE_DATA_CSV}")
            
            for index, row in df.iterrows():
                # Convert row to dictionary and handle NaN values
                transaction = row.to_dict()
                
                # Create message
                message = {
                    'transaction_id': str(transaction.get('TransactionID', f'tx_{index}')),
                    'data': transaction
                }
                
                # Send to queue
                channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
                )
                
                logger.info(f"Sent transaction {message['transaction_id']}")
                time.sleep(1)  # Small delay between messages
                
        else:
            logger.error(f"Sample data file not found: {SAMPLE_DATA_CSV}")
        
        connection.close()
        logger.info("All transactions sent successfully!")
        
    except Exception as e:
        logger.error(f"Error sending transactions: {e}")

if __name__ == "__main__":
    send_sample_transactions()

