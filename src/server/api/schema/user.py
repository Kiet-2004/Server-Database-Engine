from pydantic import BaseModel, Field



class UserCreate(BaseModel):
    user_name: str | None = None
    password: str = Field(min_length=8, max_length=40)

class UserLoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
