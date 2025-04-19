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

# Function to process payment events based on their type
def process_payment_event(event):
    if not event or "event_type" not in event:
        return
    
    event_type = event["event_type"]
    timestamp = event.get("timestamp", str(datetime.now()))
    payment_id = event.get("payment_id", "unknown")
    order_id = event.get("order_id", "unknown")
    amount = event.get("amount", "N/A")
    status = event.get("status", "unknown")
    
    if event_type == "payment_created":
        print(f"[{timestamp}] 💰 NEW PAYMENT: Payment {payment_id} created for order {order_id} with amount {amount}")
        # Additional processing like updating analytics, notifications, etc.
        
    elif event_type == "payment_processed":
        print(f"[{timestamp}] ✅ PAYMENT PROCESSED: Payment {payment_id} processed with status {status}")
        # Additional processing for payment processing
        
    elif event_type == "payment_failed":
        print(f"[{timestamp}] ❌ PAYMENT FAILED: Payment {payment_id} failed with reason: {event.get('reason', 'unknown')}")
        # Additional processing for payment failures
        
    elif event_type == "payment_refunded":
        print(f"[{timestamp}] ↩️ PAYMENT REFUNDED: Payment {payment_id} was refunded, amount: {event.get('refund_amount', 'unknown')}")
        # Processing for refunds
        
    elif event_type == "payment_status_updated":
        print(f"[{timestamp}] 🔄 PAYMENT STATUS UPDATED: Payment {payment_id} status changed to {status}")
        # Processing for status updates
        
    elif event_type == "payment_method_added":
        print(f"[{timestamp}] ➕ PAYMENT METHOD ADDED: New payment method {event.get('method_name', 'unknown')} added for payment {payment_id}")
        # Processing for payment method additions
        
    elif event_type == "payment_viewed":
        print(f"[{timestamp}] 👁️ PAYMENT VIEWED: Payment {payment_id} was viewed from IP {event.get('ip_address', 'unknown')}")
        # Logging for analytics
        
    elif event_type == "invoice_generated":
        print(f"[{timestamp}] 📄 INVOICE GENERATED: Invoice generated for payment {payment_id}")
        # Processing for invoice generation
        
    elif event_type == "payment_reminder_sent":
        print(f"[{timestamp}] 📢 PAYMENT REMINDER SENT: Reminder sent for payment {payment_id}")
        # Processing for payment reminders
        
    elif event_type == "payment_disputed":
        print(f"[{timestamp}] ⚠️ PAYMENT DISPUTED: Payment {payment_id} was disputed, reason: {event.get('dispute_reason', 'unknown')}")
        # Processing for payment disputes
        
    elif event_type == "payment_settled":
        print(f"[{timestamp}] 🏦 PAYMENT SETTLED: Payment {payment_id} was settled, settlement ID: {event.get('settlement_id', 'unknown')}")
        # Processing for payment settlements
        
    else:
        print(f"[{timestamp}] ❓ UNKNOWN PAYMENT EVENT: {event_type}")
        print(f"Event data: {event}")

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'payment-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=deserialize_message,
    api_version=(0, 10),
    consumer_timeout_ms=60000  # Wait up to 60 seconds for new messages
)

# Consume and process events
print("Payment Events Consumer started. Waiting for messages...")
print("------------------------------------------------------")
try:
    for message in consumer:
        if message.value is not None:
            process_payment_event(message.value)
        else:
            print("Received message with None value, skipping")
except KeyboardInterrupt:
    print("\nConsumer stopped by user")
    consumer.close()
except Exception as e:
    print(f"Error consuming messages: {e}")
    consumer.close()