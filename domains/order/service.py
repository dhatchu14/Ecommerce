from sqlalchemy.orm import Session
from domains.order.repository import OrderRepository
from domains.order.schema import OrderCreateSchema, OrderUpdate
from domains.order.models import Order, OrderItem
from domains.order.aggregates import OrderAggregate

class OrderService:
    def __init__(self, db: Session):
        self.db = db
        self.order_repo = OrderRepository(db)

    def create_order(self, order_data: OrderCreateSchema):
        # Create the order
        order = Order(
            status=order_data.status,
            payment_status=order_data.payment_status,
            shipment_status=order_data.shipment_status,
            customer_id=order_data.customer_id
        )

        # Create order items
        for item in order_data.items:
            order_item = OrderItem(
                product_id=item.product_id,
                quantity=item.quantity,
                unit_price=item.unit_price  # Make sure this matches your model
            )
            order.items.append(order_item)

        # Use the repository to save the order
        created_order = self.order_repo.create_order(order)

        # Return the aggregate
        aggregate = OrderAggregate(
            created_order, created_order.items, created_order.status, 
            created_order.payment_status, created_order.shipment_status
        )
        return aggregate.order

    def get_order(self, order_id: int):
        order = self.order_repo.get_order(order_id)
        if not order:
            return None
        
        # Handle the case where tracking might not exist
        tracking = order.tracking if hasattr(order, 'tracking') else None
        
        # Create the aggregate
        aggregate = OrderAggregate(
            order, order.items, order.status, order.payment_status, 
            order.shipment_status, tracking
        )
        return aggregate.order

    def update_order(self, order_id: int, order_data: OrderUpdate):
        order = self.order_repo.get_order(order_id)
        if not order:
            return None
        
        update_data = {}
        if order_data.status:
            update_data["status"] = order_data.status
        
        updated_order = self.order_repo.update_order(order_id, update_data)
        return updated_order

    def delete_order(self, order_id: int):
        return self.order_repo.delete_order(order_id)