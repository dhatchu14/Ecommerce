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

# Function to process inventory events based on their type
def process_inventory_event(event):
    if not event or "event_type" not in event:
        return
    
    event_type = event["event_type"]
    timestamp = event.get("timestamp", str(datetime.now()))
    inventory_id = event.get("inventory_id", "unknown")
    name = event.get("name", "unknown")
    sku = event.get("sku", "unknown")
    quantity = event.get("quantity", "N/A")
    
    if event_type == "inventory_created":
        print(f"[{timestamp}] 🆕 NEW INVENTORY: Item {name} (SKU: {sku}) created with ID {inventory_id}")
        # Additional processing like updating analytics, notifications, etc.
        
    elif event_type == "inventory_updated":
        print(f"[{timestamp}] ✏️ INVENTORY UPDATED: Item {name} (ID: {inventory_id}) was updated")
        # Additional processing for updates
        
    elif event_type == "inventory_deleted":
        print(f"[{timestamp}] 🗑️ INVENTORY DELETED: Item {name} (ID: {inventory_id}) was deleted")
        # Additional processing for deletions
        
    elif event_type == "inventory_viewed":
        print(f"[{timestamp}] 👁️ INVENTORY VIEWED: Item {name} (ID: {inventory_id}) was viewed from IP {event.get('ip_address', 'unknown')}")
        # Logging for analytics
        
    elif event_type == "inventory_item_added":
        print(f"[{timestamp}] ➕ ITEM ADDED: New item added to inventory ID {inventory_id}")
        # Processing for item additions
        
    elif event_type == "stock_updated":
        print(f"[{timestamp}] 📦 STOCK UPDATED: Item {name} (ID: {inventory_id}) stock level changed to {quantity}")
        # Processing for stock updates - possible alerts for low stock
        
    elif event_type == "warehouse_created":
        print(f"[{timestamp}] 🏭 WAREHOUSE CREATED: Warehouse {name} (ID: {inventory_id}) was created")
        # Processing for warehouse creation
        
    elif event_type == "warehouse_updated":
        print(f"[{timestamp}] 🏭 WAREHOUSE UPDATED: Warehouse {name} (ID: {inventory_id}) was updated")
        # Processing for warehouse updates
        
    elif event_type == "warehouse_deleted":
        print(f"[{timestamp}] 🏭 WAREHOUSE DELETED: Warehouse {name} (ID: {inventory_id}) was deleted")
        # Processing for warehouse deletion
        
    elif event_type == "warehouse_viewed":
        print(f"[{timestamp}] 🏭 WAREHOUSE VIEWED: Warehouse {name} (ID: {inventory_id}) was viewed")
        # Processing for warehouse views
        
    elif event_type == "replenishment_order_created":
        print(f"[{timestamp}] 📋 REPLENISHMENT ORDER CREATED: New order for inventory ID {inventory_id}")
        # Processing for replenishment order creation
        
    elif event_type == "replenishment_order_updated":
        print(f"[{timestamp}] 📋 REPLENISHMENT ORDER UPDATED: Order updated for inventory ID {inventory_id}")
        # Processing for replenishment order updates
    
    else:
        print(f"[{timestamp}] ❓ UNKNOWN INVENTORY EVENT: {event_type}")
        print(f"Event data: {event}")

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'inventory-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=deserialize_message,
    api_version=(0, 10),
    consumer_timeout_ms=60000  # Wait up to 60 seconds for new messages
)

# Consume and process events
print("Inventory Events Consumer started. Waiting for messages...")
print("--------------------------------------------------------")
try:
    for message in consumer:
        if message.value is not None:
            process_inventory_event(message.value)
        else:
            print("Received message with None value, skipping")
except KeyboardInterrupt:
    print("\nConsumer stopped by user")
    consumer.close()
except Exception as e:
    print(f"Error consuming messages: {e}")
    consumer.close()