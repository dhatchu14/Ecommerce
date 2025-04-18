from sqlalchemy.orm import Session
from fastapi import HTTPException
import logging
from starlette.requests import Request
from starlette.responses import RedirectResponse

from .repository import UserRepository
from .schemas import RegisterUser, LoginUser
from .security import verify_password

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class AuthService:
    def __init__(self, db: Session):
        self.db = db
        if db:
            self.user_repository = UserRepository(db)

    def register_user(self, user_data: RegisterUser):
        existing_user = self.user_repository.get_user_by_email(user_data.email)
        if existing_user:
            raise HTTPException(status_code=400, detail="User already exists!")
        return self.user_repository.create_user(user_data)

    def login_user(self, request: Request, login_data: LoginUser):
        try:
            # Debug logging
            logger.debug(f"Attempting login for email: {login_data.email}")
            
            user = self.user_repository.get_user_by_email(login_data.email)
            
            if not user:
                logger.debug("User not found in database")
                raise HTTPException(status_code=401, detail="Invalid email or password")
            
            # Debug - print the actual field values
            logger.debug(f"User found: ID={user.id}, Email={user.email}")
            logger.debug(f"Stored password hash: {user.hashed_password}")
            
            # Verify the password
            is_valid = verify_password(login_data.password, user.hashed_password)
            logger.debug(f"Password verification result: {is_valid}")
            
            if not is_valid:
                logger.debug("Password verification failed")
                raise HTTPException(status_code=401, detail="Invalid email or password")
            
            # Store user info in session
            request.session["user"] = {
                "id": user.id,
                "email": user.email,
                "username": user.username
            }
            logger.debug("User successfully logged in and session created")
            
            return {
                "message": "Login successful",
                "user": {
                    "email": user.email,
                    "username": user.username
                }
            }
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            # Log the actual error
            logger.error(f"Unexpected error during login: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    def logout_user(self, request: Request):
        request.session.clear()
        return RedirectResponse(url="/", status_code=303)

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user