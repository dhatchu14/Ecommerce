# domains/order/schemas.py - Fix for OrderItemSchema

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from domains.order.models import OrderPaymentStatusEnum, OrderShipmentStatusEnum, OrderStatusEnum

# Fixed OrderItemSchema with unit_price
class OrderItemSchema(BaseModel):
    product_id: int
    quantity: int
    unit_price: float  # This field was missing but is used in service.py

# Product Schemas
class ProductCreate(BaseModel):
    name: str
    price: float

class ProductResponse(ProductCreate):
    id: int

    class Config:
        from_attributes = True

# Order Item Schemas
class OrderItemCreate(BaseModel):
    product_id: int
    quantity: int
    unit_price: float

class OrderItemResponse(OrderItemCreate):
    id: int
    subtotal: float

    class Config:
        from_attributes = True

class OrderCreateSchema(BaseModel):
    customer_id: int  
    status: OrderStatusEnum = OrderStatusEnum.PENDING
    payment_status: OrderPaymentStatusEnum = OrderPaymentStatusEnum.PENDING
    shipment_status: OrderShipmentStatusEnum = OrderShipmentStatusEnum.NOT_SHIPPED
    items: List[OrderItemSchema]

class OrderUpdate(BaseModel):
    status: Optional[OrderStatusEnum] = None  

class OrderResponse(BaseModel):
    id: int
    customer_id: int
    status: OrderStatusEnum
    payment_status: OrderPaymentStatusEnum
    shipment_status: OrderShipmentStatusEnum

    class Config:
        from_attributes = True

class OrderSchema(BaseModel):
    id: int
    customer_id: int
    status: OrderStatusEnum
    payment_status: OrderPaymentStatusEnum
    shipment_status: OrderShipmentStatusEnum

    class Config:
        from_attributes = True

# Inventory Schemas
class InventoryBase(BaseModel):
    name: str
    description: Optional[str] = None
    quantity: int

class InventoryCreate(InventoryBase):
    pass

class InventorySchema(InventoryBase):
    id: int

    class Config:
        from_attributes = True  

class InventoryUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[int] = None