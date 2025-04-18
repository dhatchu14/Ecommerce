from sqlalchemy.orm import Session
from domains.customer.models import Customer
from domains.customer.schemas import CustomerUpdate
from domains.customer.security import hash_password

class CustomerRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_customer_by_id(self, customer_id: int):
        return self.db.query(Customer).filter(Customer.id == customer_id).first()

    def get_customer_by_email(self, email: str):
        return self.db.query(Customer).filter(Customer.email == email).first()

    def update_customer(self, customer_id: int, update_data: CustomerUpdate):
        customer = self.get_customer_by_id(customer_id)

        if not customer:
            return None

        if update_data.name:
            customer.name = update_data.name
        if update_data.contact_number:
            customer.contact_number = update_data.contact_number
        if update_data.email:
            customer.email = update_data.email

        self.db.commit()
        self.db.refresh(customer)
        return customer

    def reset_password(self, email: str, new_password: str):
        customer = self.get_customer_by_email(email)

        if not customer:
         return None

        customer.hashed_password = hash_password(new_password)  # ✅ Fixed field name
        self.db.commit()
        return customer



    def deactivate_account(self, email: str):
        customer = self.get_customer_by_email(email)

        if not customer:
            return None

        customer.is_active = False  # Soft delete (Mark inactive)
        self.db.commit()
        return customer