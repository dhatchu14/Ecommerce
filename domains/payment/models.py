from sqlalchemy import Column, Integer, Float, ForeignKey, String, Enum as SQLAlchemyEnum, DateTime
from sqlalchemy.orm import relationship
from db import Base
from datetime import datetime
from domains.payment.aggregates import PaymentStatus, PaymentMethod

class PaymentModel(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    amount = Column(Float, nullable=False)
    # Use the existing type and specify name
    status = Column(SQLAlchemyEnum(PaymentStatus, name="paymentstatusenum", create_type=False), 
                    default=PaymentStatus.PENDING, nullable=False)
    method = Column(SQLAlchemyEnum(PaymentMethod, name="paymentmethodenum", create_type=False), 
                    nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    transactions = relationship("TransactionModel", back_populates="payment", cascade="all, delete-orphan")
    invoice = relationship("InvoiceModel", uselist=False, back_populates="payment", cascade="all, delete-orphan")
    refund = relationship("RefundModel", uselist=False, back_populates="payment", cascade="all, delete-orphan")

class TransactionModel(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    payment_id = Column(Integer, ForeignKey("payments.id"), nullable=False)
    transaction_id = Column(String, unique=True, nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(SQLAlchemyEnum(PaymentStatus, name="paymentstatusenum", create_type=False), 
                    nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    payment = relationship("PaymentModel", back_populates="transactions")

class InvoiceModel(Base):
    __tablename__ = "invoices"

    id = Column(Integer, primary_key=True, index=True)
    payment_id = Column(Integer, ForeignKey("payments.id"), nullable=False)
    invoice_number = Column(String, unique=True, nullable=False)
    amount = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    payment = relationship("PaymentModel", back_populates="invoice")

class RefundModel(Base):
    __tablename__ = "refunds"

    id = Column(Integer, primary_key=True, index=True)
    payment_id = Column(Integer, ForeignKey("payments.id"), nullable=False)
    refund_amount = Column(Float, nullable=False)
    status = Column(SQLAlchemyEnum(PaymentStatus, name="paymentstatusenum", create_type=False), 
                    nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    payment = relationship("PaymentModel", back_populates="refund")