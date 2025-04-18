from sqlalchemy.orm import Session
from fastapi import HTTPException
from domains.inventory.repository import InventoryRepository
from domains.inventory.models import Inventory, InventoryItem, StockLevel, Warehouse, ReplenishmentOrder
from domains.inventory.schemas import InventoryCreate, InventoryUpdate, WarehouseCreate, WarehouseUpdate, ReplenishmentOrderCreate

class InventoryService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = InventoryRepository(db)
    
    def create_inventory(self, inventory_data: InventoryCreate):
        inventory = Inventory(warehouse_id=inventory_data.warehouse_id)
        return self.repository.create_inventory(inventory)
    
    def get_inventory(self, inventory_id: int):
        inventory = self.repository.get_inventory(inventory_id)
        if not inventory:
            raise HTTPException(status_code=404, detail="Inventory not found")
        return inventory
    
    def update_inventory(self, inventory_id: int, inventory_data: InventoryUpdate):
        inventory = self.repository.update_inventory(inventory_id, inventory_data.dict(exclude_unset=True))
        if not inventory:
            raise HTTPException(status_code=404, detail="Inventory not found")
        return inventory
    
    def delete_inventory(self, inventory_id: int):
        inventory = self.repository.delete_inventory(inventory_id)
        if not inventory:
            raise HTTPException(status_code=404, detail="Inventory not found")
        return None
    
    def add_inventory_item(self, inventory_id: int, item_data: dict):
        inventory = self.repository.get_inventory(inventory_id)
        if not inventory:
            raise HTTPException(status_code=404, detail="Inventory not found")
        
        new_item = InventoryItem(
            inventory_id=inventory_id,
            product_id=item_data["product_id"],
            quantity=item_data["quantity"],
            price=item_data.get("price")
        )
        
        self.db.add(new_item)
        self.db.commit()
        self.db.refresh(new_item)
        
        stock_level = StockLevel(
            item_id=new_item.id,
            quantity=new_item.quantity
        )
        
        self.db.add(stock_level)
        self.db.commit()
        
        return new_item
    
    def update_stock_level(self, item_id: int, stock_data: dict):
        item = self.db.query(InventoryItem).filter(InventoryItem.id == item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")
        
        stock_level = self.db.query(StockLevel).filter(StockLevel.item_id == item_id).first()
        if not stock_level:
            stock_level = StockLevel(item_id=item_id, quantity=stock_data["quantity"])
            self.db.add(stock_level)
        else:
            stock_level.quantity = stock_data["quantity"]
        
        item.quantity = stock_data["quantity"]
        
        self.db.commit()
        self.db.refresh(item)
        return item
    
    def create_replenishment_order(self, inventory_id: int, order_data: ReplenishmentOrderCreate):
        # Check if inventory exists
        inventory = self.repository.get_inventory(inventory_id)
        if not inventory:
            raise ValueError(f"Inventory with ID {inventory_id} not found")
        
        # Create a new replenishment order
        new_order = ReplenishmentOrder(
            inventory_id=inventory_id,
            quantity=order_data.quantity,
            status=order_data.status
        )
        
        # Add to database
        self.db.add(new_order)
        self.db.commit()
        self.db.refresh(new_order)
        
        return new_order
    
    def update_replenishment_order(self, inventory_id: int, order_id: int, order_data: ReplenishmentOrderCreate):
        """
        Update an existing replenishment order
        """
        # First check if the inventory exists
        inventory = self.repository.get_inventory(inventory_id)
        if not inventory:
            raise ValueError(f"Inventory with ID {inventory_id} not found")
        
        # Find the existing order
        order = self.db.query(ReplenishmentOrder).filter(
            ReplenishmentOrder.id == order_id,
            ReplenishmentOrder.inventory_id == inventory_id
        ).first()
        
        if not order:
            raise ValueError(f"Replenishment order with ID {order_id} not found in inventory {inventory_id}")
        
        # Update the order
        order.quantity = order_data.quantity
        order.status = order_data.status
        
        # If item_id is provided, update it
        if hasattr(order_data, 'item_id') and order_data.item_id is not None:
            order.item_id = order_data.item_id
        
        # Save changes
        self.db.commit()
        self.db.refresh(order)
        
        return order

class WarehouseService:
    def __init__(self, db: Session):
        self.db = db
    
    def create_warehouse(self, warehouse_data: WarehouseCreate):
        warehouse = Warehouse(name=warehouse_data.name, location=warehouse_data.location)
        self.db.add(warehouse)
        self.db.commit()
        self.db.refresh(warehouse)
        return warehouse
    
    def get_warehouse(self, warehouse_id: int):
        warehouse = self.db.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not warehouse:
            raise HTTPException(status_code=404, detail="Warehouse not found")
        return warehouse
    
    def update_warehouse(self, warehouse_id: int, warehouse_data: WarehouseUpdate):
        warehouse = self.db.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not warehouse:
            raise HTTPException(status_code=404, detail="Warehouse not found")
        
        update_data = warehouse_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(warehouse, key, value)
        
        self.db.commit()
        self.db.refresh(warehouse)
        return warehouse
    
    def delete_warehouse(self, warehouse_id: int):
        warehouse = self.db.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not warehouse:
            raise HTTPException(status_code=404, detail="Warehouse not found")
        
        self.db.delete(warehouse)
        self.db.commit()
        return None