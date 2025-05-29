# Authentication middleware
from fastapi import Depends, HTTPException, status
import jwt

from server.config.settings import SECRET_KEY, ALGORITHM
from server.database.user_db import USER_DATABASE as UserDB
from server.utils.security import *

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "access":
            raise credentials_exception
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception

    query = UserDB.get_user(user_name=username) 
    if query is None:
        raise credentials_exception
    return query

def refresh_access_token(access_token: str, refresh_token: str):
    # Check if access token is expired
    if not is_access_token_expired(access_token):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Access token is still valid and cannot be refreshed",
        )
    
    # Verify refresh token
    username = verify_refresh_token(refresh_token)
    user = UserDB.get_user(user_name=username)  # Assuming UserDB is defined
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Generate new tokens
    new_access_token = create_access_token(data={"sub": username})
    new_refresh_token = create_refresh_token(data={"sub": username})  # Rotate refresh token
    return Token(
        access_token=new_access_token,
        refresh_token=new_refresh_token,
        token_type="bearer"
    )