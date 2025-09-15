import pika
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RESULTS_QUEUE = "fraud_results_queue"

def view_results(ch, method, properties, body):
    """Display fraud detection results"""
    try:
        result = json.loads(body)
        
        print("\n" + "="*60)
        print("🔍 FRAUD DETECTION RESULT")
        print("="*60)
        print(f"Transaction ID: {result.get('transaction_id', 'N/A')}")
        
        is_fraud = result.get('is_fraud', False)
        probability = result.get('fraud_probability', 0)
        
        if is_fraud:
            print(f"🚨 STATUS: FRAUD DETECTED")
            print(f"🎯 Confidence: {probability:.1%}")
        else:
            print(f"✅ STATUS: LEGITIMATE TRANSACTION")
            print(f"🎯 Fraud Risk: {probability:.1%}")
        
        print("="*60)
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(f"Error displaying result: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    """Main function to view results"""
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        # Declare results queue
        channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        
        # Start consuming results
        channel.basic_consume(queue=RESULTS_QUEUE, on_message_callback=view_results)
        
        print("\n🎯 Fraud Detection Results Viewer")
        print(f"📨 Listening for results on '{RESULTS_QUEUE}' queue")
        print("Press CTRL+C to exit\n")
        
        channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n🛑 Results viewer stopped by user")
        channel.stop_consuming()
        connection.close()
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()
