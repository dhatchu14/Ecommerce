from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette.requests import Request

from domains.inventory.models import InventoryItem, StockLevel
from domains.inventory.service import InventoryService, WarehouseService
from domains.inventory.schemas import InventoryCreate, InventoryItemCreate, InventoryItemSchema, InventorySchema, InventoryUpdate, ReplenishmentOrderCreate, ReplenishmentOrderSchema, StockUpdateSchema, WarehouseCreate, WarehouseSchema, WarehouseUpdate
from db import get_db

# Create three separate routers
inventory_router = APIRouter()  # For inventory endpoints
products_router = APIRouter()   # For product-specific endpoints
warehouse_router = APIRouter()  # For warehouse endpoints

# Warehouse Endpoints
@warehouse_router.post("/", response_model=WarehouseSchema, status_code=201)
def create_warehouse(warehouse_data: WarehouseCreate, db: Session = Depends(get_db)):
    try:
        warehouse_service = WarehouseService(db)
        warehouse = warehouse_service.create_warehouse(warehouse_data)
        return warehouse
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@warehouse_router.get("/{warehouse_id}", response_model=WarehouseSchema)
def get_warehouse(warehouse_id: int, db: Session = Depends(get_db)):
    warehouse_service = WarehouseService(db)
    return warehouse_service.get_warehouse(warehouse_id)

@warehouse_router.put("/{warehouse_id}", response_model=WarehouseSchema)
def update_warehouse(warehouse_id: int, warehouse_data: WarehouseUpdate, db: Session = Depends(get_db)):
    warehouse_service = WarehouseService(db)
    return warehouse_service.update_warehouse(warehouse_id, warehouse_data)

@warehouse_router.delete("/{warehouse_id}", status_code=204)
def delete_warehouse(warehouse_id: int, db: Session = Depends(get_db)):
    warehouse_service = WarehouseService(db)
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
def create_inventory(inventory_data: InventoryCreate, db: Session = Depends(get_db)):
    inventory_service = InventoryService(db)
    return inventory_service.create_inventory(inventory_data)

@inventory_router.get("/{inventory_id}", response_model=InventorySchema)
def get_inventory(inventory_id: int, db: Session = Depends(get_db)):
    inventory_service = InventoryService(db)
    return inventory_service.get_inventory(inventory_id)

@inventory_router.put("/{inventory_id}", response_model=InventorySchema)
def update_inventory(inventory_id: int, inventory_data: InventoryUpdate, db: Session = Depends(get_db)):
    inventory_service = InventoryService(db)
    return inventory_service.update_inventory(inventory_id, inventory_data)

@inventory_router.delete("/{inventory_id}", status_code=204)
def delete_inventory(inventory_id: int, db: Session = Depends(get_db)):
    inventory_service = InventoryService(db)
    return inventory_service.delete_inventory(inventory_id)

@inventory_router.put("/{inventory_id}/items", response_model=InventoryItemSchema)
def add_inventory_item(inventory_id: int, item_data: InventoryItemCreate, db: Session = Depends(get_db)):
    try:
        inventory_service = InventoryService(db)
        item = inventory_service.add_inventory_item(inventory_id, item_data.dict())
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
def update_stock_level(item_id: int, stock_data: StockUpdateSchema, db: Session = Depends(get_db)):
    try:
        inventory_service = InventoryService(db)
        item = inventory_service.update_stock_level(item_id, stock_data.dict())
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
def create_replenishment_order(inventory_id: int, order_data: ReplenishmentOrderCreate, db: Session = Depends(get_db)):
    try:
        inventory_service = InventoryService(db)
        order = inventory_service.create_replenishment_order(inventory_id, order_data)
        return order
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@inventory_router.put("/{inventory_id}/orders/{order_id}", response_model=ReplenishmentOrderSchema)
def update_replenishment_order(inventory_id: int, order_id: int, order_data: ReplenishmentOrderCreate, db: Session = Depends(get_db)):
    try:
        inventory_service = InventoryService(db)
        order = inventory_service.update_replenishment_order(inventory_id, order_id, order_data)
        return order
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
