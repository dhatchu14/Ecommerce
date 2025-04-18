from sqlalchemy import Column, Float, Integer, String, ForeignKey, Enum
from sqlalchemy.orm import relationship
from db import Base
import enum

# Order Status Enum
class OrderStatusEnum(str, enum.Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELED = "canceled"

# Payment Status Enum
class OrderPaymentStatusEnum(str, enum.Enum):
    PENDING = "pending"
    PAID = "paid"
    FAILED = "failed"
    REFUNDED = "refunded"

# Shipment Status Enum
class OrderShipmentStatusEnum(str, enum.Enum):
    NOT_SHIPPED = "not_shipped"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    RETURNED = "returned"

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    status = Column(Enum(OrderStatusEnum), default=OrderStatusEnum.PENDING)
    payment_status = Column(Enum(OrderPaymentStatusEnum), default=OrderPaymentStatusEnum.PENDING)
    shipment_status = Column(Enum(OrderShipmentStatusEnum), default=OrderShipmentStatusEnum.NOT_SHIPPED)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    
    items = relationship("OrderItem", back_populates="order")
    tracking = relationship("OrderTracking", back_populates="order", uselist=False)  # One-to-One
    customer = relationship("Customer", back_populates="orders")
    # Removed the customer_preferences relationship

class OrderItem(Base):
    __tablename__ = "order_items"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"))
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    
    order = relationship("Order", back_populates="items")

class OrderTracking(Base):
    __tablename__ = "order_tracking"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"))
    customer_id = Column(Integer, ForeignKey("customers.id"))
    tracking_number = Column(String, nullable=False)
    carrier = Column(String, nullable=False)
    status = Column(String, default="In Transit")
    
    order = relationship("Order", back_populates="tracking")
    customer = relationship("Customer", back_populates="tracking_orders")