from sqlalchemy.orm import Session
from .models import User
from .schemas import RegisterUser
from .security import hash_password

class UserRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_user(self, user_data: RegisterUser):
        hashed_pw = hash_password(user_data.password)
        new_user = User(username=user_data.username, email=user_data.email, hashed_password=hashed_pw)
        self.db.add(new_user)
        self.db.commit()
        self.db.refresh(new_user)
        return new_user

    def get_user_by_email(self, email: str):
        return self.db.query(User).filter(User.email == email).first()