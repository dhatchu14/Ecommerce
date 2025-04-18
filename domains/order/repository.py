# domains/order/repository.py
from sqlalchemy.orm import Session
from domains.order.models import Order

class OrderRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_order(self, order: Order):
        try:
            self.db.add(order)
            self.db.commit()
            self.db.refresh(order)
            return order
        except Exception as e:
            self.db.rollback()
            raise e

    def get_order(self, order_id: int):
        return self.db.query(Order).filter(Order.id == order_id).first()

    def update_order(self, order_id: int, update_data: dict):
        order = self.get_order(order_id)
        if not order:
            return None
        
        for key, value in update_data.items():
            setattr(order, key, value)
        
        self.db.commit()
        self.db.refresh(order)
        return order

    def delete_order(self, order_id: int):
        order = self.get_order(order_id)
        if not order:
            return False
        
        self.db.delete(order)
        self.db.commit()
        return True