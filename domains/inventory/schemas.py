from pydantic import BaseModel
from typing import Optional, List
from enum import Enum

# Import the OrderStatusEnum from your models
from domains.inventory.models import OrderStatusEnum

# Base Inventory Schema
class InventoryBase(BaseModel):
    warehouse_id: int

# Create Schema - used when creating a new inventory
class InventoryCreate(InventoryBase):
    pass

# Update Schema - used when updating an inventory
class InventoryUpdate(BaseModel):
    warehouse_id: Optional[int] = None

# Inventory Item Schemas
class InventoryItemCreate(BaseModel):
    product_id: int
    quantity: int
    price: Optional[float] = None

class InventoryItemSchema(BaseModel):
    id: int
    product_id: int
    quantity: int
    price: Optional[float] = None
    
    class Config:
        from_attributes = True

# Stock Level Schemas
class StockUpdateSchema(BaseModel):
    quantity: int

class StockLevelSchema(BaseModel):
    id: int
    item_id: int
    quantity: int
    
    class Config:
        from_attributes = True

# Replenishment Order Schemas
class ReplenishmentOrderCreate(BaseModel):
    item_id: Optional[int] = None
    quantity: int
    status: OrderStatusEnum = OrderStatusEnum.PENDING

class ReplenishmentOrderSchema(BaseModel):
    id: int
    inventory_id: int
    quantity: int
    status: OrderStatusEnum
    
    class Config:
        from_attributes = True

# Final Inventory Schema (Includes Inventory Items)
class InventorySchema(BaseModel):
    id: int
    warehouse_id: int
    items: List[InventoryItemSchema] = []
    
    class Config:
        from_attributes = True

# Warehouse Schemas
class WarehouseBase(BaseModel):
    name: str
    location: str

class WarehouseCreate(WarehouseBase):
    pass

class WarehouseUpdate(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None

class WarehouseSchema(WarehouseBase):
    id: int

    class Config:
        from_attributes = True