from kafka import KafkaProducer
import json
import time

# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10),  # Explicitly set API version
    retries=5  # Number of retries if sending fails
)

# Function to send data to Kafka
def send_event_to_kafka(event_data):
    try:
        # Validate event data
        if not isinstance(event_data, dict) or "event_type" not in event_data:
            print(f"Invalid event data format: {event_data}")
            return False
            
        # Add timestamp if not present
        if "timestamp" not in event_data:
            from datetime import datetime
            event_data["timestamp"] = str(datetime.now())
            
        # Send the event and get the future result
        future = producer.send('user-events', value=event_data)
        
        # Wait for the result to ensure delivery (with timeout)
        record_metadata = future.get(timeout=10)
        
        print(f"Event sent to Kafka: {event_data['event_type']} for {event_data.get('username', 'unknown user')}")
        return True
    except Exception as e:
        print(f"Failed to send event to Kafka: {e}")
        return False