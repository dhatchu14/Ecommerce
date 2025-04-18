from sqlalchemy.orm import Session
from domains.inventory.models import Inventory

class InventoryRepository:
    def __init__(self, db: Session):
        self.db = db
    
    def create_inventory(self, inventory: Inventory):
        self.db.add(inventory)
        self.db.commit()
        self.db.refresh(inventory)
        return inventory
    
    def get_inventory(self, inventory_id: int):
        return self.db.query(Inventory).filter(Inventory.id == inventory_id).first()
    
    def update_inventory(self, inventory_id: int, inventory_data: dict):
        inventory = self.get_inventory(inventory_id)
        if inventory:
            for key, value in inventory_data.items():
                setattr(inventory, key, value)
            self.db.commit()
        return inventory
    
    def delete_inventory(self, inventory_id: int):
        inventory = self.get_inventory(inventory_id)
        if inventory:
            self.db.delete(inventory)
            self.db.commit()
        return inventory