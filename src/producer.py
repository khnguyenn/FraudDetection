import pika 
import json
import csv
import time
import os

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = "fraud_detection_queue"

# New Application For Production 
NEW_APPLICATIONS_CSV = 


def create_queue(channel, queue_name):
    # Create queue if not exists
    try:
        channel.queue_declare(queue=queue_name, durable = True)
    except Exception as e:
        print(f"Error creating queue {queue_name}")


