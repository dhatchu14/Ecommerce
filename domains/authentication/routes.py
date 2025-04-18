from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.responses import JSONResponse
import datetime
from db import get_db
from .service import AuthService, get_current_user
from .schemas import RegisterUser, LoginUser
from producer.auth_producer import send_event_to_kafka

router = APIRouter()

@router.post("/register")
def register(user_data: RegisterUser, db: Session = Depends(get_db)):
    try:
        # Register the user using the existing AuthService
        auth_service = AuthService(db)
        new_user = auth_service.register_user(user_data)

        # Create the event data to send to Kafka
        event_data = {
            "event_type": "user_registered",
            "username": user_data.username,
            "email": user_data.email,
            
        }

        # Send the event to Kafka and check result
        kafka_result = send_event_to_kafka(event_data)
        
        # Return response
        if kafka_result:
            return {"message": "User registered successfully and event sent to Kafka", "user": event_data}
        else:
            # Still return success but note the Kafka issue
            return {
                "message": "User registered successfully but event publishing had issues", 
                "user": {"username": user_data.username, "email": user_data.email}
            }
    except Exception as e:
        # Log the error
        print(f"Error in register endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.post("/login")
def login(request: Request, login_data: LoginUser, db: Session = Depends(get_db)):
    try:
        # Process login with the auth service
        auth_service = AuthService(db)
        login_result = auth_service.login_user(request, login_data)
        
        # If login was successful, send event to Kafka
        if "user" in login_result:
            event_data = {
                "event_type": "user_login",
                "username": login_result["user"]["username"],
                "email": login_result["user"]["email"],
                
            }
            
            send_event_to_kafka(event_data)
            
        return login_result
    except Exception as e:
        print(f"Error in login endpoint: {str(e)}")
        raise

@router.get("/me")
def get_profile(user: dict = Depends(get_current_user), request: Request = None):
    try:
        # Track profile view events
        event_data = {
            "event_type": "profile_viewed",
            "username": user["username"],
            "email": user["email"],
            
        }
        
        # Asynchronously send event - don't wait for result since this is just analytics
        send_event_to_kafka(event_data)
        
        return JSONResponse(content={"username": user["username"], "email": user["email"]})
    except Exception as e:
        print(f"Error tracking profile view: {str(e)}")
        # Still return the profile data even if tracking failed
        return JSONResponse(content={"username": user["username"], "email": user["email"]})

@router.post("/logout")
def logout(request: Request):
    try:
        # Get user before logout
        user = None
        try:
            user = request.session.get("user")
        except:
            pass
            
        # Process logout
        auth_service = AuthService(None)
        logout_result = auth_service.logout_user(request)
        
        # If we had a user in session, track the logout
        if user:
            event_data = {
                "event_type": "user_logout",
                "username": user.get("username", "unknown"),
                "email": user.get("email", "unknown"),
                
                
            }
            
            send_event_to_kafka(event_data)
            
        return logout_result
    except Exception as e:
        print(f"Error in logout endpoint: {str(e)}")
        raise