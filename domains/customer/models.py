from sqlalchemy import Column, Integer, String, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from db import Base

class Address(Base):
    __tablename__ = "addresses"
    
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    street = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zip_code = Column(String, nullable=False)

    customer = relationship("Customer", back_populates="addresses")

class Customer(Base):
    __tablename__ = "customers"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False, index=True)
    contact_number = Column(String, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Relationships
    addresses = relationship("Address", back_populates="customer")
    preferences = relationship("CustomerPreference", back_populates="customer")
    orders = relationship("Order", back_populates="customer")
    tracking_orders = relationship("OrderTracking", back_populates="customer")

class CustomerPreference(Base):
    __tablename__ = "customer_preferences"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"))
    preference_data = Column(String)

    customer = relationship("Customer", back_populates="preferences")
    # Removed the order relationship