from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:root@localhost:5432/ecommerce2")

# Create SQLAlchemy Engine
engine = create_engine(DATABASE_URL)

# Base class for models
Base = declarative_base()

# Session Maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



def reset_database():
    """Drop existing tables and recreate them."""
    try:
        with engine.connect() as connection:
            connection.execute(text("DROP SCHEMA public CASCADE;"))
            connection.execute(text("CREATE SCHEMA public;"))
            connection.execute(text("GRANT ALL ON SCHEMA public TO postgres;"))
            connection.execute(text("GRANT ALL ON SCHEMA public TO public;"))
            connection.commit()
        
        print("Database schema reset successfully")

        # ✅ Ensure all tables are created
        create_tables()

    except Exception as e:
        print(f"Error during database reset: {e}")
        raise

def create_tables():
    """Create all tables based on the defined SQLAlchemy models."""
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

# ✅ Automatically create tables when the module is loaded
create_tables()

# Dependency for database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
