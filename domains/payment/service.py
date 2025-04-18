from fastapi import HTTPException
from sqlalchemy.orm import Session
from domains.payment.repository import PaymentRepository
from domains.payment.schema import PaymentCreateSchema
from domains.payment.aggregates import PaymentStatus, PaymentMethod
from domains.payment.models import PaymentModel, TransactionModel, InvoiceModel, RefundModel
from sqlalchemy.exc import SQLAlchemyError

class PaymentService:
    @staticmethod
    def process_payment(db: Session, payment_data: PaymentCreateSchema):
        try:
            # Create the payment record first
            db_payment = PaymentModel(
                order_id=payment_data.order_id,
                amount=payment_data.amount,
                method=payment_data.method,
                status=PaymentStatus.PENDING
            )
            db.add(db_payment)
            db.flush()  # Get the payment ID
            
            # Generate a transaction ID
            transaction_id = f"TXN-{db_payment.id}-{int(db_payment.created_at.timestamp())}"
            
            # Create a transaction record
            transaction = TransactionModel(
                payment_id=db_payment.id,
                transaction_id=transaction_id,
                amount=db_payment.amount,
                status=PaymentStatus.PENDING
            )
            db.add(transaction)
            
            # Generate invoice number and create invoice
            invoice_number = f"INV-{db_payment.id}-{int(db_payment.created_at.timestamp())}"
            invoice = InvoiceModel(
                payment_id=db_payment.id,
                invoice_number=invoice_number,
                amount=db_payment.amount
            )
            db.add(invoice)
            
            # Simulate payment gateway logic
            if payment_data.method in [PaymentMethod.CREDIT_CARD, PaymentMethod.DEBIT_CARD, PaymentMethod.PAYPAL]:
                db_payment.status = PaymentStatus.COMPLETED
                transaction.status = PaymentStatus.COMPLETED
            
            db.commit()
            db.refresh(db_payment)
            return db_payment
        
        except SQLAlchemyError as e:
            db.rollback()
            # Log the error for debugging
            print(f"Database error occurred: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        except Exception as e:
            db.rollback()
            # Log the error for debugging
            print(f"Unexpected error occurred: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    @staticmethod
    def get_payment_details(db: Session, payment_id: int):
        return db.query(PaymentModel).filter(PaymentModel.id == payment_id).first()

    @staticmethod
    def update_payment_status(db: Session, payment_id: int, status: PaymentStatus):
        try:
            payment = db.query(PaymentModel).filter(PaymentModel.id == payment_id).first()
            if payment:
                payment.status = status
                
                # Update associated transaction status
                for transaction in payment.transactions:
                    transaction.status = status
                
                db.commit()
                db.refresh(payment)
            return payment
        except Exception as e:
            db.rollback()
            raise e