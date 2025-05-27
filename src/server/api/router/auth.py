from typing import Any, Annotated
import fastapi
from fastapi import Depends, Body
from fastapi.security import OAuth2PasswordRequestForm

from server.api.schema.user import UserCreate, UserLoginResponse, RefreshRequest
from server.controllers.user_controller import create_user, login_user
from server.database.db_engine import engine
from server.middleware.auth import Token, refresh_access_token
# from fastapi.security import OAuth2PasswordRequestForm


router = fastapi.APIRouter(prefix="/auth", tags=["Authentication"])

@router.post('/sigin') 
def sigin(user: UserCreate) -> Any:
    """
    check empty string
    """
    user = create_user(user_name=user.user_name, password=user.password)
    return user

@router.post('/login', response_model=UserLoginResponse)
def login(form: OAuth2PasswordRequestForm = Depends()):
    """
    check empty
    """
    jwt_payload = login_user(user_name=form.username, password=form.password)
    return jwt_payload

@router.post('/connect', response_model=UserLoginResponse)
def connect(db_name: str, form: OAuth2PasswordRequestForm = Depends()):
    """
    check empty
    """
    jwt_payload = login_user(user_name=form.username, password=form.password)
    engine.load_db(form.username, db_name)
    return jwt_payload

@router.post("/refresh", response_model=Token)
def refresh(request: RefreshRequest):
    return refresh_access_token(request.access_token, request.refresh_token)

