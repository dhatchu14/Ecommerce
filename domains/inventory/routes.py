from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import datetime
from starlette.requests import Request

from domains.inventory.models import InventoryItem, StockLevel
from domains.inventory.service import InventoryService, WarehouseService
from domains.inventory.schemas import InventoryCreate, InventoryItemCreate, InventoryItemSchema, InventorySchema, InventoryUpdate, ReplenishmentOrderCreate, ReplenishmentOrderSchema, StockUpdateSchema, WarehouseCreate, WarehouseSchema, WarehouseUpdate
from db import get_db
from producer.inventory_producer import send_event_to_kafka  # Import the Kafka producer function

# Create three separate routers
inventory_router = APIRouter()  # For inventory endpoints
products_router = APIRouter()   # For product-specific endpoints
warehouse_router = APIRouter()  # For warehouse endpoints

# Custom function to send inventory events to Kafka
def send_inventory_event(event_type, inventory_data, request=None):
    try:
        # Extract client IP if request is available
        ip_address = "unknown"
        if request and hasattr(request, "client") and hasattr(request.client, "host"):
            ip_address = request.client.host
            
        # Create event data
        event_data = {
            "event_type": event_type,
            "inventory_id": getattr(inventory_data, "id", None),
            "name": getattr(inventory_data, "name", None),
            "sku": getattr(inventory_data, "sku", None),
            "quantity": getattr(inventory_data, "quantity", None),
            "timestamp": str(datetime.datetime.now()),
            "ip_address": ip_address
        }
        
        # Send to Kafka with topic explicitly specified
        return send_event_to_kafka(event_data, topic="inventory-events")
    except Exception as e:
        print(f"Error sending inventory event to Kafka: {str(e)}")
        return False

# Warehouse Endpoints
@warehouse_router.post("/", response_model=WarehouseSchema, status_code=201)
def create_warehouse(warehouse_data: WarehouseCreate, db: Session = Depends(get_db), request: Request = None):
    try:
        warehouse_service = WarehouseService(db)
        warehouse = warehouse_service.create_warehouse(warehouse_data)
        
        # Send warehouse created event to Kafka
        send_inventory_event("warehouse_created", warehouse, request)
        
        return warehouse
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@warehouse_router.get("/{warehouse_id}", response_model=WarehouseSchema)
def get_warehouse(warehouse_id: int, db: Session = Depends(get_db), request: Request = None):
    warehouse_service = WarehouseService(db)
    warehouse = warehouse_service.get_warehouse(warehouse_id)
    
    # Send warehouse viewed event to Kafka
    send_inventory_event("warehouse_viewed", warehouse, request)
    
    return warehouse

@warehouse_router.put("/{warehouse_id}", response_model=WarehouseSchema)
def update_warehouse(warehouse_id: int, warehouse_data: WarehouseUpdate, db: Session = Depends(get_db), request: Request = None):
    warehouse_service = WarehouseService(db)
    warehouse = warehouse_service.update_warehouse(warehouse_id, warehouse_data)
    
    # Send warehouse updated event to Kafka
    send_inventory_event("warehouse_updated", warehouse, request)
    
    return warehouse

@warehouse_router.delete("/{warehouse_id}", status_code=204)
def delete_warehouse(warehouse_id: int, db: Session = Depends(get_db), request: Request = None):
    warehouse_service = WarehouseService(db)
    warehouse = warehouse_service.get_warehouse(warehouse_id)
    
    # Send warehouse deleted event to Kafka before deletion
    if warehouse:
        send_inventory_event("warehouse_deleted", warehouse, request)
        
    return warehouse_service.delete_warehouse(warehouse_id)

@warehouse_router.get("/test_db", status_code=200)
def test_db(db: Session = Depends(get_db)):
    try:
        # Simple query to test database connection
        result = db.execute("SELECT 1").scalar()
        return {"status": "success", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Inventory endpoints
@inventory_router.post("/", status_code=201, response_model=InventorySchema)
def create_inventory(inventory_data: InventoryCreate, db: Session = Depends(get_db), request: Request = None):
    inventory_service = InventoryService(db)
    inventory = inventory_service.create_inventory(inventory_data)
    
    # Send inventory created event to Kafka
    send_inventory_event("inventory_created", inventory, request)
    
    return inventory

@inventory_router.get("/{inventory_id}", response_model=InventorySchema)
def get_inventory(inventory_id: int, db: Session = Depends(get_db), request: Request = None):
    inventory_service = InventoryService(db)
    inventory = inventory_service.get_inventory(inventory_id)
    
    # Send inventory viewed event to Kafka
    send_inventory_event("inventory_viewed", inventory, request)
    
    return inventory

@inventory_router.put("/{inventory_id}", response_model=InventorySchema)
def update_inventory(inventory_id: int, inventory_data: InventoryUpdate, db: Session = Depends(get_db), request: Request = None):
    inventory_service = InventoryService(db)
    inventory = inventory_service.update_inventory(inventory_id, inventory_data)
    
    # Send inventory updated event to Kafka
    send_inventory_event("inventory_updated", inventory, request)
    
    return inventory

@inventory_router.delete("/{inventory_id}", status_code=204)
def delete_inventory(inventory_id: int, db: Session = Depends(get_db), request: Request = None):
    inventory_service = InventoryService(db)
    inventory = inventory_service.get_inventory(inventory_id)
    
    # Send inventory deleted event to Kafka before deletion
    if inventory:
        send_inventory_event("inventory_deleted", inventory, request)
        
    return inventory_service.delete_inventory(inventory_id)

@inventory_router.put("/{inventory_id}/items", response_model=InventoryItemSchema)
def add_inventory_item(inventory_id: int, item_data: InventoryItemCreate, db: Session = Depends(get_db), request: Request = None):
    try:
        inventory_service = InventoryService(db)
        item = inventory_service.add_inventory_item(inventory_id, item_data.dict())
        
        # Send inventory item added event to Kafka
        send_inventory_event("inventory_item_added", item, request)
        
        return item
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error adding inventory item: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Stock level endpoint
@inventory_router.put("/items/{item_id}/stock", response_model=InventoryItemSchema)
def update_stock_level(item_id: int, stock_data: StockUpdateSchema, db: Session = Depends(get_db), request: Request = None):
    try:
        inventory_service = InventoryService(db)
        item = inventory_service.update_stock_level(item_id, stock_data.dict())
        
        # Send stock updated event to Kafka
        send_inventory_event("stock_updated", item, request)
        
        return item
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error updating stock: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Product-specific endpoints
@products_router.get("/", response_model=list[InventorySchema])
def get_all_products(db: Session = Depends(get_db)):
    inventory_service = InventoryService(db)
    # Implement a method to get all products
    return []  # Replace with actual implementation

@inventory_router.post("/{inventory_id}/orders", response_model=ReplenishmentOrderSchema)
def create_replenishment_order(inventory_id: int, order_data: ReplenishmentOrderCreate, db: Session = Depends(get_db), request: Request = None):
    try:
        inventory_service = InventoryService(db)
        order = inventory_service.create_replenishment_order(inventory_id, order_data)
        
        # Send replenishment order created event to Kafka
        send_inventory_event("replenishment_order_created", order, request)
        
        return order
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@inventory_router.put("/{inventory_id}/orders/{order_id}", response_model=ReplenishmentOrderSchema)
def update_replenishment_order(inventory_id: int, order_id: int, order_data: ReplenishmentOrderCreate, db: Session = Depends(get_db), request: Request = None):
    try:
        inventory_service = InventoryService(db)
        order = inventory_service.update_replenishment_order(inventory_id, order_id, order_data)
        
        # Send replenishment order updated event to Kafka
        send_inventory_event("replenishment_order_updated", order, request)
        
        return order
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")