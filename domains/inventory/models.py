from sqlalchemy import Column, Float, Integer, ForeignKey, String, Enum
from sqlalchemy.orm import relationship
from db import Base  # Ensure this imports your declarative Base
import enum

class OrderStatusEnum(enum.Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELED = "CANCELED"
    
class Warehouse(Base):
    __tablename__ = "warehouses"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    location = Column(String, nullable=False)
    
    # Relationship with Inventory
    inventories = relationship("Inventory", back_populates="warehouse")

class Inventory(Base):
    __tablename__ = "inventory"
    
    id = Column(Integer, primary_key=True, index=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id", ondelete="CASCADE"))  # ✅ Fixed FK reference
    warehouse = relationship("Warehouse", back_populates="inventories")  # ✅ Fixed back_populates
    
    items = relationship("InventoryItem", back_populates="inventory")
    replenishment_orders = relationship("ReplenishmentOrder", back_populates="inventory")

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    
    id = Column(Integer, primary_key=True, index=True)
    inventory_id = Column(Integer, ForeignKey("inventory.id", ondelete="CASCADE"))
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=True)  # Add this field
    
    inventory = relationship("Inventory", back_populates="items")
    stock_levels = relationship("StockLevel", back_populates="item")

class StockLevel(Base):
    __tablename__ = "stock_levels"  # ✅ Pluralized for consistency
    
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer, ForeignKey("inventory_items.id", ondelete="CASCADE"))
    quantity = Column(Integer, nullable=False)
    
    item = relationship("InventoryItem", back_populates="stock_levels")

class ReplenishmentOrder(Base):
    __tablename__ = "replenishment_orders"  # ✅ Pluralized for consistency
    
    id = Column(Integer, primary_key=True, index=True)
    inventory_id = Column(Integer, ForeignKey("inventory.id", ondelete="CASCADE"))
    quantity = Column(Integer, nullable=False)
    status = Column(Enum(OrderStatusEnum), default=OrderStatusEnum.PENDING)
    
    inventory = relationship("Inventory", back_populates="replenishment_orders")