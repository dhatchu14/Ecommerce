from enum import Enum
from datetime import datetime
from typing import List, Optional

class PaymentStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"

class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    UPI = "upi"
    BANK_TRANSFER = "bank_transfer"

class Transaction:
    def __init__(self, transaction_id: str, amount: float, status: PaymentStatus, created_at: Optional[datetime] = None):
        self.transaction_id = transaction_id
        self.amount = amount
        self.status = status
        self.created_at = created_at or datetime.utcnow()

class Invoice:
    def __init__(self, invoice_number: str, amount: float, created_at: Optional[datetime] = None):
        self.invoice_number = invoice_number
        self.amount = amount
        self.created_at = created_at or datetime.utcnow()

class Refund:
    def __init__(self, refund_amount: float, status: PaymentStatus = PaymentStatus.PENDING, created_at: Optional[datetime] = None):
        self.refund_amount = refund_amount
        self.status = status
        self.created_at = created_at or datetime.utcnow()

class Payment:
    def __init__(self, order_id: int, amount: float, method: PaymentMethod, status: PaymentStatus = PaymentStatus.PENDING, created_at: Optional[datetime] = None):
        self.order_id = order_id
        self.amount = amount
        self.method = method
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.transactions: List[Transaction] = []
        self.invoice: Optional[Invoice] = None
        self.refund: Optional[Refund] = None

    def add_transaction(self, transaction_id: str, amount: float, status: PaymentStatus):
        transaction = Transaction(transaction_id, amount, status)
        self.transactions.append(transaction)

    def generate_invoice(self, invoice_number: str):
        self.invoice = Invoice(invoice_number, self.amount)

    def process_refund(self, refund_amount: float):
        if refund_amount > self.amount:
            raise ValueError("Refund amount cannot exceed the payment amount.")
        self.refund = Refund(refund_amount, PaymentStatus.PENDING)
