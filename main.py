from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import logging

# Import authentication routes
from domains.authentication.routes import router as auth_router
from domains.customer.routes import router as customer_router

# Import database initialization (only creating tables for authentication)
from db import create_tables

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
SECRET_KEY = os.getenv("SECRET_KEY", "your_default_secret_key")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ecommerce2")

# Initialize FastAPI app
app = FastAPI(
    title="Auth API",
    description="API for user authentication testing",
    version="1.0.0"
)

# Create database tables for authentication
try:
    create_tables()
    logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating tables: {e}")
    raise

# Middleware for session handling
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# CORS Middleware Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update for production security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["Health Check"])
async def root():
    """Check API health."""
    return {"message": "Auth API is running 🚀"}

# Register Authentication Router
app.include_router(auth_router, prefix="/auth", tags=["Authentication"])
app.include_router(customer_router, prefix="/customers", tags=["Customers"])



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
