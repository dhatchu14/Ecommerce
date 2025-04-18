from typing import List
from domains.inventory.models import Inventory, InventoryItem, StockLevel, ReplenishmentOrder

class InventoryAggregate:
    def __init__(self, inventory: 'Inventory', items: List['InventoryItem'] = None, stock_levels: List['StockLevel'] = None, replenishment_orders: List['ReplenishmentOrder'] = None):
        self.inventory = inventory
        self.items = items or []
        self.stock_levels = stock_levels or []
        self.replenishment_orders = replenishment_orders or []

    def add_item(self, item: InventoryItem):
        self.items.append(item)

    def update_stock(self, item_id: int, new_stock: int):
        for stock in self.stock_levels:
            if stock.item_id == item_id:
                stock.quantity = new_stock
                break

    def add_replenishment_order(self, order: ReplenishmentOrder):
        self.replenishment_orders.append(order)
