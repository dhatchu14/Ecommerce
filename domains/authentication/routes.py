from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.responses import JSONResponse
from db import get_db
from .service import AuthService, get_current_user
from .schemas import RegisterUser, LoginUser

router = APIRouter()

@router.post("/register")
def register(user_data: RegisterUser, db: Session = Depends(get_db)):
    try:
        auth_service = AuthService(db)
        new_user = auth_service.register_user(user_data)
        return {
            "message": "User registered successfully",
            "user": {"username": user_data.username, "email": user_data.email}
        }
    except Exception as e:
        print(f"Error in register endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.post("/login")
def login(request: Request, login_data: LoginUser, db: Session = Depends(get_db)):
    try:
        auth_service = AuthService(db)
        login_result = auth_service.login_user(request, login_data)
        return login_result
    except Exception as e:
        print(f"Error in login endpoint: {str(e)}")
        raise

@router.post("/logout")
def logout(request: Request):
    try:
        auth_service = AuthService(None)
        logout_result = auth_service.logout_user(request)
        return logout_result
    except Exception as e:
        print(f"Error in logout endpoint: {str(e)}")
        raise
