from sqlalchemy.orm import Session
from domains.payment.models import PaymentModel, TransactionModel, InvoiceModel, RefundModel
from domains.payment.schema import PaymentCreateSchema
from domains.payment.aggregates import PaymentStatus

class PaymentRepository:
    @staticmethod
    def create_payment(db: Session, payment_data: PaymentCreateSchema):
        # Create the payment
        db_payment = PaymentModel(
            order_id=payment_data.order_id,
            amount=payment_data.amount,
            method=payment_data.method,
            status=PaymentStatus.PENDING
        )
        db.add(db_payment)
        db.flush()  # Get the payment ID
        
        # Create a transaction
        transaction_id = f"TXN-{db_payment.id}"
        transaction = TransactionModel(
            payment_id=db_payment.id,
            transaction_id=transaction_id,
            amount=db_payment.amount,
            status=PaymentStatus.PENDING
        )
        db.add(transaction)
        
        # Create an invoice
        invoice_number = f"INV-{db_payment.id}"
        invoice = InvoiceModel(
            payment_id=db_payment.id,
            invoice_number=invoice_number,
            amount=db_payment.amount
        )
        db.add(invoice)
        
        db.commit()
        db.refresh(db_payment)
        return db_payment

    @staticmethod
    def get_payment(db: Session, payment_id: int):
        return db.query(PaymentModel).filter(PaymentModel.id == payment_id).first()

    @staticmethod
    def update_status(db: Session, payment_id: int, status: PaymentStatus):
        try:
            payment = db.query(PaymentModel).filter(PaymentModel.id == payment_id).first()
            if payment:
                payment.status = status
                db.commit()
                db.refresh(payment)
            return payment
        except Exception as e:
            db.rollback()
            raise e
