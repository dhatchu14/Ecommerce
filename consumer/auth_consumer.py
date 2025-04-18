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

# Function to process user events based on their type
def process_event(event):
    if not event or "event_type" not in event:
        return
    
    event_type = event["event_type"]
    timestamp = event.get("timestamp", str(datetime.now()))
    
    if event_type == "user_registered":
        print(f"[{timestamp}] 👤 NEW USER: {event.get('username')} ({event.get('email')}) registered")
        # Here you could add code to send welcome emails, update analytics, etc.
        
    elif event_type == "user_login":
        print(f"[{timestamp}] 🔐 LOGIN: {event.get('username')} logged in from IP {event.get('ip_address', 'unknown')}")
        # Here you could add code for security monitoring, session tracking, etc.
        
    elif event_type == "user_logout":
        print(f"[{timestamp}] 👋 LOGOUT: {event.get('username')} logged out")
        # Here you could add code to clean up sessions, update analytics, etc.
        
    elif event_type == "profile_viewed":
        print(f"[{timestamp}] 👁️ PROFILE: {event.get('username')} viewed their profile")
        # Here you could update user activity metrics, etc.
    
    else:
        print(f"[{timestamp}] ❓ UNKNOWN EVENT TYPE: {event_type}")
        print(f"Event data: {event}")

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=deserialize_message,
    api_version=(0, 10),
    consumer_timeout_ms=60000  # Wait up to 60 seconds for new messages
)

# Consume and process events
print("User Events Consumer started. Waiting for messages...")
print("----------------------------------------------------")
try:
    for message in consumer:
        if message.value is not None:
            process_event(message.value)
        else:
            print("Received message with None value, skipping")
except KeyboardInterrupt:
    print("\nConsumer stopped by user")
    consumer.close()
except Exception as e:
    print(f"Error consuming messages: {e}")
    consumer.close()