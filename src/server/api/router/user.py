from typing import Any
import fastapi
from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm

from server.api.schema.user import UserCreate, UserLoginResponse
from server.controllers.db_controlller import select
from server.controllers.user_controller import create_user, login_user
# from fastapi.security import OAuth2PasswordRequestForm


router = fastapi.APIRouter(prefix="/users", tags=["users"])

@router.post('/sigin') 
def sigin(user: UserCreate) -> Any:
    """
    check empty string
    """
    user = create_user(user_name=user.user_name, password=user.password)
    return user

@router.post('/login', response_model=UserLoginResponse)
def login(user: OAuth2PasswordRequestForm = Depends()):
    """
    check empty
    """
    jwt_payload = login_user(user_name=user.username, password=user.password)
    return jwt_payload
