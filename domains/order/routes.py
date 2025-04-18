from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import datetime
from starlette.requests import Request

from db import get_db
from domains.order.service import OrderService
from domains.order.schema import OrderCreateSchema, OrderSchema, OrderUpdate

router = APIRouter()

@router.post("/", response_model=OrderSchema, status_code=201)
def create_order(order_data: OrderCreateSchema, db: Session = Depends(get_db), request: Request = None):
    """Create a new order"""
    try:
        order_service = OrderService(db)
        db_order = order_service.create_order(order_data)
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
    return db_order

@router.put("/{order_id}", response_model=OrderSchema)
def update_order(order_id: int, order_data: OrderUpdate, db: Session = Depends(get_db), request: Request = None):
    """Update an order"""
    order_service = OrderService(db)
    db_order = order_service.update_order(order_id, order_data)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order

@router.put("/{order_id}/status", response_model=OrderSchema)
def update_order_status(order_id: int, status: str, db: Session = Depends(get_db), request: Request = None):
    """Update an order's status"""
    order_service = OrderService(db)
    db_order = order_service.update_order_status(order_id, status)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order

@router.delete("/{order_id}", status_code=204)
def delete_order(order_id: int, db: Session = Depends(get_db), request: Request = None):
    """Delete an order"""
    order_service = OrderService(db)
    result = order_service.delete_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return None  # Proper return for 204 No Content response
