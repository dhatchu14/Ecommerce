from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import datetime
from starlette.requests import Request

from db import get_db
from domains.order.service import OrderService
from domains.order.schema import OrderCreateSchema, OrderSchema, OrderUpdate
from producer.order_producer import send_event_to_kafka  # Import the Kafka producer function

router = APIRouter()

# Custom function to send order events to Kafka
def send_order_event(event_type, order_data, request=None):
    try:
        # Extract client IP if request is available
        ip_address = "unknown"
        if request and hasattr(request, "client") and hasattr(request.client, "host"):
            ip_address = request.client.host
            
        # Create event data
        event_data = {
            "event_type": event_type,
            "order_id": getattr(order_data, "id", None),
            "customer_id": getattr(order_data, "customer_id", None),
            "total_amount": getattr(order_data, "total_amount", None),
            "status": getattr(order_data, "status", None),
            "timestamp": str(datetime.datetime.now()),
            "ip_address": ip_address
        }
        
        # Send to Kafka with topic explicitly specified
        return send_event_to_kafka(event_data, topic="order-events")
    except Exception as e:
        print(f"Error sending order event to Kafka: {str(e)}")
        return False

@router.post("/", response_model=OrderSchema, status_code=201)
def create_order(order_data: OrderCreateSchema, db: Session = Depends(get_db), request: Request = None):
    """Create a new order"""
    try:
        order_service = OrderService(db)
        db_order = order_service.create_order(order_data)
        
        # Send order created event to Kafka
        send_order_event("order_created", db_order, request)
        
        return db_order
    except Exception as e:
        print(f"Error creating order: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{order_id}", response_model=OrderSchema)
def get_order(order_id: int, db: Session = Depends(get_db), request: Request = None):
    """Retrieve an order by ID"""
    order_service = OrderService(db)
    db_order = order_service.get_order(order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Send order viewed event to Kafka
    send_order_event("order_viewed", db_order, request)
    
    return db_order

@router.put("/{order_id}", response_model=OrderSchema)
def update_order(order_id: int, order_data: OrderUpdate, db: Session = Depends(get_db), request: Request = None):
    """Update an order"""
    order_service = OrderService(db)
    db_order = order_service.update_order(order_id, order_data)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Send order updated event to Kafka
    send_order_event("order_updated", db_order, request)
    
    return db_order

@router.put("/{order_id}/status", response_model=OrderSchema)
def update_order_status(order_id: int, status: str, db: Session = Depends(get_db), request: Request = None):
    """Update an order's status"""
    order_service = OrderService(db)
    db_order = order_service.update_order_status(order_id, status)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Send order status updated event to Kafka
    send_order_event("order_status_updated", db_order, request)
    
    return db_order

@router.delete("/{order_id}", status_code=204)
def delete_order(order_id: int, db: Session = Depends(get_db), request: Request = None):
    """Delete an order"""
    order_service = OrderService(db)
    
    # Get the order before deletion to send in the event
    db_order = order_service.get_order(order_id)
    if db_order:
        # Send order deleted event to Kafka
        send_order_event("order_deleted", db_order, request)
    
    result = order_service.delete_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    # No return value for 204 response