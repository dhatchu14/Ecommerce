from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from domains.payment.aggregates import PaymentStatus, PaymentMethod

class TransactionSchema(BaseModel):
    transaction_id: str
    amount: float
    status: PaymentStatus
    created_at: datetime
    
    class Config:
        from_attributes = True

class InvoiceSchema(BaseModel):
    invoice_number: str
    amount: float
    created_at: datetime
    
    class Config:
        from_attributes = True

class RefundSchema(BaseModel):
    refund_amount: float
    status: PaymentStatus
    created_at: datetime
    
    class Config:
        from_attributes = True

class PaymentCreateSchema(BaseModel):
    order_id: int
    amount: float
    method: PaymentMethod

class PaymentResponseSchema(BaseModel):
    id: int
    order_id: int
    amount: float
    method: PaymentMethod
    status: PaymentStatus
    created_at: datetime
    transactions: List[TransactionSchema] = []
    invoice: Optional[InvoiceSchema] = None
    refund: Optional[RefundSchema] = None
    
    class Config:
        from_attributes = True

class PaymentRequest(BaseModel):
    order_id: int
    amount: float
    method: str
class PaymentStatusUpdate(BaseModel):
    status: PaymentStatus
