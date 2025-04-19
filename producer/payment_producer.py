from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Function to handle serialization with error checking
def serialize_message(msg):
    try:
        return json.dumps(msg).encode('utf-8')
    except Exception as e:
        print(f"Error serializing message: {e}")
        return None

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=serialize_message,
    api_version=(0, 10),
    retries=5
)

# Function to publish payment events
def publish_payment_event(event_type, payment_data):
    try:
        # Create event with common fields
        event = {
            "event_type": event_type,
            "timestamp": str(datetime.now()),
            **payment_data
        }
        
        # Send to payment-events topic
        producer.send('payment-events', event)
        producer.flush()  # Ensure the message is sent immediately
        
        print(f"Published event: {event_type} with data: {payment_data}")
        return True
    except Exception as e:
        print(f"Error publishing payment event: {e}")
        return False

# Example usage functions
def publish_payment_created(payment_id, order_id, amount, status):
    payment_data = {
        "payment_id": payment_id,
        "order_id": order_id,
        "amount": amount,
        "status": status
    }
    return publish_payment_event("payment_created", payment_data)

def publish_payment_processed(payment_id, order_id, amount, status):
    payment_data = {
        "payment_id": payment_id,
        "order_id": order_id,
        "amount": amount,
        "status": status
    }
    return publish_payment_event("payment_processed", payment_data)

def publish_payment_failed(payment_id, order_id, reason):
    payment_data = {
        "payment_id": payment_id,
        "order_id": order_id,
        "reason": reason
    }
    return publish_payment_event("payment_failed", payment_data)

def publish_payment_refunded(payment_id, order_id, refund_amount, status):
    payment_data = {
        "payment_id": payment_id,
        "order_id": order_id,
        "refund_amount": refund_amount,
        "status": status
    }
    return publish_payment_event("payment_refunded", payment_data)

def publish_payment_status_updated(payment_id, order_id, status):
    payment_data = {
        "payment_id": payment_id,
        "order_id": order_id,
        "status": status
    }
    return publish_payment_event("payment_status_updated", payment_data)

# Test function
def test_kafka_connection():
    try:
        test_event = {
            "event_type": "payment_test",
            "timestamp": str(datetime.now()),
            "message": "Kafka connection test"
        }
        producer.send('payment-events', test_event)
        producer.flush()
        print("Kafka connection test successful")
        return True
    except Exception as e:
        print(f"Kafka connection test failed: {e}")
        return False

# Run a test if this module is executed directly
if __name__ == "__main__":
    test_kafka_connection()
    
    # Example test event
    test_publish = publish_payment_created(
        payment_id=1001,
        order_id=5001,
        amount=99.99,
        status="PENDING"
    )
    
    if test_publish:
        print("Test event published successfully")
    else:
        print("Failed to publish test event")
        
    # Close producer when done
    producer.close()