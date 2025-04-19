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

# Function to process customer events based on their type
def process_customer_event(event):
    if not event or "event_type" not in event:
        return
    
    event_type = event["event_type"]
    timestamp = event.get("timestamp", str(datetime.now()))
    customer_id = event.get("customer_id", "unknown")
    email = event.get("email", "unknown")
    name = f"{event.get('first_name', '')} {event.get('last_name', '')}".strip() or "unknown"
    
    if event_type == "customer_registered":
        print(f"[{timestamp}] 🆕 NEW CUSTOMER: {name} ({email}) registered with ID {customer_id}")
        # Here you could add code to send welcome emails, update analytics, etc.
        
    elif event_type == "customer_created":
        print(f"[{timestamp}] ✅ CUSTOMER CREATED: {name} ({email}) created with ID {customer_id}")
        # Here you could add code for onboarding workflows, notifications, etc.
        
    elif event_type == "customer_viewed":
        print(f"[{timestamp}] 👁️ CUSTOMER VIEWED: Customer {name} (ID: {customer_id}) profile was viewed from IP {event.get('ip_address', 'unknown')}")
        # Here you could log access for security/audit purposes
    
    else:
        print(f"[{timestamp}] ❓ UNKNOWN CUSTOMER EVENT: {event_type}")
        print(f"Event data: {event}")

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'customer-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=deserialize_message,
    api_version=(0, 10),
    consumer_timeout_ms=60000  # Wait up to 60 seconds for new messages
)

# Consume and process events
print("Customer Events Consumer started. Waiting for messages...")
print("--------------------------------------------------------")
try:
    for message in consumer:
        if message.value is not None:
            process_customer_event(message.value)
        else:
            print("Received message with None value, skipping")
except KeyboardInterrupt:
    print("\nConsumer stopped by user")
    consumer.close()
except Exception as e:
    print(f"Error consuming messages: {e}")
    consumer.close()