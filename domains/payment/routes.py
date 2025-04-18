from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime

from db import get_db
from domains.payment.schema import PaymentCreateSchema, PaymentResponseSchema, PaymentStatusUpdate
from domains.payment.service import PaymentService
from domains.payment.aggregates import PaymentStatus

router = APIRouter()

@router.post("/", response_model=PaymentResponseSchema, operation_id="create_payment_unique")
def create_payment(payment: PaymentCreateSchema, db: Session = Depends(get_db)):
    try:
        payment_result = PaymentService.process_payment(db, payment)
        return payment_result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/{payment_id}", response_model=PaymentResponseSchema)
def get_payment(payment_id: int, db: Session = Depends(get_db)):
    payment = PaymentService.get_payment_details(db, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment

@router.put("/{payment_id}")
def update_payment_status(
    payment_id: int,
    status: PaymentStatus = Body(..., embed=True),
    db: Session = Depends(get_db)
):
    payment = PaymentService.update_payment_status(db, payment_id, status)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment

@router.post("/{payment_id}/process")
def process_payment_transaction(payment_id: int, db: Session = Depends(get_db)):
    payment = PaymentService.get_payment_details(db, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    try:
        processed_payment = PaymentService.process_transaction(db, payment_id)
        return processed_payment
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing payment: {str(e)}")

@router.post("/{payment_id}/refund")
def refund_payment(
    payment_id: int,
    refund_amount: float = Body(..., embed=True),
    db: Session = Depends(get_db)
):
    payment = PaymentService.get_payment_details(db, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    try:
        refunded_payment = PaymentService.refund_payment(db, payment_id, refund_amount)
        return refunded_payment
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing refund: {str(e)}")

@router.get("/test")
def test_payment_route():
    # Test the Kafka connection by sending a test event
    try:
        test_event = {
            "message": "Payment route and Kafka connection test"
        }
        result = publish_payment_event("payment_test", test_event)
        if result:
            return {"message": "Payment route and Kafka connection working"}
        else:
            return {"message": "Payment route working but Kafka event publishing failed"}
    except Exception as e:
        return {"message": f"Payment route working but Kafka connection failed: {str(e)}"}
