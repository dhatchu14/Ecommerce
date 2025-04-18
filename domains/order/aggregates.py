# domains/order/aggregate.py (or aggregates.py)
from typing import List
from domains.order.models import Order, OrderItem, OrderStatusEnum, OrderPaymentStatusEnum, OrderShipmentStatusEnum, OrderTracking

class OrderAggregate:
    def __init__(self, order: Order, items: List[OrderItem] = None, status: OrderStatusEnum = None,
                 payment_status: OrderPaymentStatusEnum = None, shipment_status: OrderShipmentStatusEnum = None,
                 tracking: OrderTracking = None):
        self.order = order
        self.items = items or []
        self.status = status or OrderStatusEnum.PENDING
        self.payment_status = payment_status or OrderPaymentStatusEnum.PENDING
        self.shipment_status = shipment_status or OrderShipmentStatusEnum.NOT_SHIPPED
        self.tracking = tracking  # Ensure tracking is properly handled


    def add_item(self, item: OrderItem):
        self.items.append(item)

    def update_status(self, new_status: OrderStatusEnum):
        self.status = new_status

    def update_payment_status(self, new_status: OrderPaymentStatusEnum):
        self.payment_status = new_status

    def update_shipment_status(self, new_status: OrderShipmentStatusEnum):
        self.shipment_status = new_status

    def set_tracking_info(self, tracking: OrderTracking):
        self.tracking = tracking