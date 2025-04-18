from pydantic import BaseModel, EmailStr

class CustomerCreate(BaseModel):
    name: str
    email: EmailStr
    contact_number: str
    password: str

class CustomerResponse(BaseModel):
    id: int
    name: str
    email: EmailStr
    contact_number: str

    model_config = {
        "from_attributes": True
    }

class AddressSchema(BaseModel):
    street: str
    city: str
    country: str

 
class CustomerUpdate(BaseModel):
    name: str | None = None
    email: EmailStr | None = None
    contact_number: str | None = None
    password: str | None = None

class PasswordReset(BaseModel):
    old_password: str
    new_password: str

class CustomerCreateResponse(BaseModel):
    message: str
    customer: CustomerResponse

class RegisterUser(BaseModel):
    name: str
    email: EmailStr
    contact_number: str
    password: str