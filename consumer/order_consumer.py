from kafka import KafkaConsumer
import json
import time
from datetime import datetime

# Function to handle deserialization with error checking
def deserialize_message(msg):
    try:
        if msg is None:
            print("Received None message")
            return None
        return json.loads(msg.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}, Raw message: {msg!r}")
        return None
    except Exception as e:
        print(f"Error deserializing message: {e}, Raw message: {msg!r}")
        return None

# Function to process order events based on their type
def process_order_event(event):
    if not event or "event_type" not in event:
        return
    
    event_type = event["event_type"]
    timestamp = event.get("timestamp", str(datetime.now()))
    order_id = event.get("order_id", "unknown")
    customer_id = event.get("customer_id", "unknown")
    total_amount = event.get("total_amount", "N/A")
    status = event.get("status", "unknown")
    
    if event_type == "order_created":
        print(f"[{timestamp}] 🆕 NEW ORDER: Order {order_id} created by customer {customer_id} for ${total_amount}")
        # Here you could add code to send order confirmation emails, update inventory, etc.
        
    elif event_type == "order_viewed":
        print(f"[{timestamp}] 👁️ ORDER VIEWED: Order {order_id} was viewed from IP {event.get('ip_address', 'unknown')}")
        # Here you could log for analytics, fraud detection, etc.
        
    elif event_type == "order_updated":
        print(f"[{timestamp}] ✏️ ORDER UPDATED: Order {order_id} was updated")
        # Here you could log changes, notify customer of changes, etc.
        
    elif event_type == "order_status_updated":
        print(f"[{timestamp}] 📊 ORDER STATUS CHANGED: Order {order_id} status changed to '{status}'")
        # Here you could send notifications based on status changes, update shipping info, etc.
        
    elif event_type == "order_deleted":
        print(f"[{timestamp}] 🗑️ ORDER DELETED: Order {order_id} was deleted")
        # Here you could log for compliance, update reporting, etc.
    
    else:
        print(f"[{timestamp}] ❓ UNKNOWN ORDER EVENT: {event_type}")
        print(f"Event data: {event}")

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'order-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=deserialize_message,
    api_version=(0, 10),
    consumer_timeout_ms=60000  # Wait up to 60 seconds for new messages
)

# Consume and process events
print("Order Events Consumer started. Waiting for messages...")
print("--------------------------------------------------------")
try:
    for message in consumer:
        if message.value is not None:
            process_order_event(message.value)
        else:
            print("Received message with None value, skipping")
except KeyboardInterrupt:
    print("\nConsumer stopped by user")
    consumer.close()
except Exception as e:
    print(f"Error consuming messages: {e}")
    consumer.close()