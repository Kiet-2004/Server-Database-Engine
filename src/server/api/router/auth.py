from typing import Any, Annotated
import fastapi
from fastapi import Depends, Body
from fastapi.security import OAuth2PasswordRequestForm

from server.api.schema.user import UserCreate, UserLoginResponse, RefreshRequest
from server.controllers.user_controller import create_user, login_user
from server.controllers import db_controlller
from server.middleware.auth import Token, refresh_access_token, get_current_user
from server.utils.exceptions import dpapi2_exception


router = fastapi.APIRouter(prefix="/auth", tags=["Authentication"])

@router.post('/sigin') 
async def sigin(user: UserCreate) -> Any:
    """
    check empty string
    """
    user = await create_user(user_name=user.user_name, password=user.password)
    return user

@router.post('/login', response_model=UserLoginResponse)
async def login(form: OAuth2PasswordRequestForm = Depends()):
    """
    check empty
    """
    jwt_payload = await login_user(user_name=form.username, password=form.password)
    return jwt_payload

@router.post('/connect', response_model=UserLoginResponse)
async def connect(db_name: str, form: OAuth2PasswordRequestForm = Depends()):
    """
    check empty
    """
    jwt_payload = await login_user(user_name=form.username, password=form.password)
    db_controlller.connect_user(user_name=form.username, db_name=db_name)
    return jwt_payload

@router.post("/refresh", response_model=Token)
def refresh(request: RefreshRequest):
    return refresh_access_token(request.access_token, request.refresh_token)

@router.get('/disconnect')
def disconnect(current_user = Depends(get_current_user)):
    """
    Close the database connection for the given user.
    """

    return db_controlller.disconnect_user(current_user.user_name)


