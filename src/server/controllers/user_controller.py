from fastapi import HTTPException, status
from server.database.user_db import USER_DATABASE as UserDB
from server.middleware.auth import *

def create_user(user_name, password):
    """
    - check exsist user
    - hash password
    - Save db
    """
    query = UserDB.get_user(user_name=user_name)
    if query:
        raise Exception("User already exists")
    
    hased_password = hash_password(password)
    user = UserDB.add_user(user_name=user_name, password=hased_password)
    if user:
        return {
            "user_name": user_name
        }
    else:
        raise Exception("Failed to create user")


def login_user(user_name, password):
    """
    - check with hashed password
    - return jwt
    """
    query = UserDB.get_user(user_name=user_name)
    if query is None or not verify_password(password, query.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    access_token = create_access_token(data={"sub": user_name})
    refresh_token = create_refresh_token(data={"sub": user_name})
    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")