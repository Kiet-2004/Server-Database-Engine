from typing import Any
import fastapi
from fastapi import Depends
from server.api.schema.user import UserCreate, UserLoginRequest, UserLoginResponse
from server.middleware.auth import authe
from server.controllers.db_controlller import select
from server.controllers.user_controller import create_user, login_user
# from fastapi.security import OAuth2PasswordRequestForm


router = fastapi.APIRouter(prefix="/user", tags=["user"])




@router.post('/sigin') 
def sigin(user: UserCreate) -> Any:
    """
    check empty string
    """
    create_user(user_name=user.user_name, password=user.password)

    return None

@router.post('/login', response_model=UserLoginResponse)
def login(user: UserLoginRequest):
    """
    check empty
    """
    jwt = login_user(user_name=user.user_name, password=user.password)
    return None
