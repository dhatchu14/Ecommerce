from sqlalchemy.orm import Session
from domains.customer.repository import CustomerRepository
from domains.customer.schemas import CustomerUpdate, PasswordReset, CustomerCreate
from .models import Customer
from .security import get_hashed_password 

class CustomerService:
    def __init__(self, db: Session):
        self.repo = CustomerRepository(db)
        self.db = db

    def get_customer_by_email(self, email: str) -> Customer | None:
        """
        Get a customer by their email address
        """
        return self.db.query(Customer).filter(Customer.email == email).first()

    def create_customer(self, customer: CustomerCreate) -> Customer:
        """
        Create a new customer
        """
        hashed_password = get_hashed_password (customer.password)
        db_customer = Customer(
            name=customer.name,
            email=customer.email,
            contact_number=customer.contact_number,
            hashed_password=hashed_password  # Changed from hashed_password to password_hash
        )
        
        self.db.add(db_customer)
        self.db.commit()
        self.db.refresh(db_customer)
        
        return db_customer