from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime

from db import get_db
from domains.payment.schema import PaymentCreateSchema, PaymentResponseSchema, PaymentStatusUpdate
from domains.payment.service import PaymentService
from domains.payment.aggregates import PaymentStatus

# Import the Kafka producer functions from the separate module
from producer.payment_producer import (
    publish_payment_created,
    publish_payment_processed,
    publish_payment_failed,
    publish_payment_refunded,
    publish_payment_status_updated,
    publish_payment_event
)

# Remove the prefix here since it's already defined in main.py
router = APIRouter()

@router.post("/", response_model=PaymentResponseSchema, operation_id="create_payment_unique")
def create_payment(payment: PaymentCreateSchema, db: Session = Depends(get_db)):
    try:
        payment_result = PaymentService.process_payment(db, payment)
        
        # Publish payment created event using the dedicated producer
        publish_payment_created(
            payment_id=payment_result.id,
            order_id=payment_result.order_id,
            amount=payment_result.amount,
            status=payment_result.status.value if hasattr(payment_result.status, 'value') else str(payment_result.status)
        )
        
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
    
    # Publish payment viewed event
    publish_payment_event("payment_viewed", {
        "payment_id": payment.id,
        "order_id": payment.order_id
    })
    
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
    
    # Publish payment status updated event
    publish_payment_status_updated(
        payment_id=payment.id,
        order_id=payment.order_id,
        status=payment.status.value if hasattr(payment.status, 'value') else str(payment.status)
    )
    
    return payment

@router.post("/{payment_id}/process")
def process_payment_transaction(payment_id: int, db: Session = Depends(get_db)):
    payment = PaymentService.get_payment_details(db, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    try:
        # Process the payment
        processed_payment = PaymentService.process_transaction(db, payment_id)
        
        # Publish appropriate event based on result
        if processed_payment.status == PaymentStatus.COMPLETED:
            publish_payment_processed(
                payment_id=processed_payment.id,
                order_id=processed_payment.order_id,
                amount=processed_payment.amount,
                status=processed_payment.status.value if hasattr(processed_payment.status, 'value') else str(processed_payment.status)
            )
        else:
            publish_payment_failed(
                payment_id=processed_payment.id,
                order_id=processed_payment.order_id,
                reason="Payment processing did not complete successfully"
            )
        
        return processed_payment
    except Exception as e:
        # Publish payment failed event
        publish_payment_failed(
            payment_id=payment.id,
            order_id=payment.order_id,
            reason=str(e)
        )
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
        # Perform refund logic
        refunded_payment = PaymentService.refund_payment(db, payment_id, refund_amount)
        
        # Publish payment refunded event
        publish_payment_refunded(
            payment_id=payment.id,
            order_id=payment.order_id,
            refund_amount=refund_amount,
            status=refunded_payment.status.value if hasattr(refunded_payment.status, 'value') else str(refunded_payment.status)
        )
        
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