from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import datetime
from starlette.requests import Request

from domains.customer.models import Customer
from .schemas import CustomerCreate, CustomerResponse, RegisterUser, CustomerCreateResponse
from .service import CustomerService
from db import get_db
from producer.customer_producer import send_event_to_kafka  # Import the Kafka producer function

router = APIRouter(
    prefix="/customer",
    tags=["customers"]
)

# Custom function to send customer events to Kafka
def send_customer_event(event_type, customer_data, request=None):
    try:
        # Extract client IP if request is available
        ip_address = "unknown"
        if request and hasattr(request, "client") and hasattr(request.client, "host"):
            ip_address = request.client.host
            
        # Create event data
        event_data = {
            "event_type": event_type,
            "customer_id": getattr(customer_data, "id", None),
            "email": getattr(customer_data, "email", None),
            "first_name": getattr(customer_data, "first_name", None),
            "last_name": getattr(customer_data, "last_name", None),
            "timestamp": str(datetime.datetime.now()),
            "ip_address": ip_address
        }
        
        # Send to Kafka with topic explicitly specified
        return send_event_to_kafka(event_data, topic="customer-events")
    except Exception as e:
        print(f"Error sending customer event to Kafka: {str(e)}")
        return False

@router.post("/register", response_model=CustomerCreateResponse)
async def register_customer(user_data: RegisterUser, db: Session = Depends(get_db), request: Request = None):
    try:
        service = CustomerService(db)
        existing_customer = service.get_customer_by_email(user_data.email)
        
        if existing_customer:
            raise HTTPException(status_code=400, detail="Email already registered")
        
        created_customer = service.create_customer(user_data)
        
        # Send customer registration event to Kafka
        send_customer_event("customer_registered", created_customer, request)
        
        return CustomerCreateResponse(
            message="Customer registered successfully!",
            customer=created_customer
        )
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        print(f"Error in register_customer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.post("/", response_model=CustomerCreateResponse)
def create_customer(customer: CustomerCreate, db: Session = Depends(get_db), request: Request = None):
    try:
        service = CustomerService(db)
        existing_customer = service.get_customer_by_email(customer.email)

        if existing_customer:
            raise HTTPException(status_code=400, detail="Email already registered")

        created_customer = service.create_customer(customer)
        
        # Send customer creation event to Kafka
        send_customer_event("customer_created", created_customer, request)
        
        return CustomerCreateResponse(
            message="Customer registered successfully!",
            customer=created_customer
        )
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        print(f"Error in create_customer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Customer creation failed: {str(e)}")

@router.get("/{customer_id}", response_model=CustomerResponse)
def get_customer(customer_id: int, db: Session = Depends(get_db), request: Request = None):
    try:
        service = CustomerService(db)
        customer = db.query(Customer).filter(Customer.id == customer_id).first()

        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
            
        # Send customer viewed event to Kafka
        send_customer_event("customer_viewed", customer, request)
        
        return customer
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        print(f"Error in get_customer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving customer: {str(e)}")