from pydantic import BaseModel, EmailStr, field_validator
from pydantic.types import Annotated
from pydantic import StringConstraints

class RegisterUser(BaseModel):
    username: str
    email: EmailStr
    password: Annotated[str, StringConstraints(min_length=8)] # type: ignore

    @field_validator('password')  
    @classmethod
    def validate_password(cls, v: str) -> str:
        if not any(c.isalpha() for c in v):
            raise ValueError("Password must contain at least one letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one number")
        return v

class LoginUser(BaseModel):
    email: EmailStr
    password: str