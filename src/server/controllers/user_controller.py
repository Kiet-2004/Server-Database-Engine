from fastapi import HTTPException, status
from server.database.user_db import USER_DATABASE as UserDB
from server.middleware.auth import *
from server.utils.exceptions import dpapi2_exception

async def create_user(user_name: str, password: str):
    """
    - check exsist user
    - hash password
    - Save db
    """
    # check empty
    if not user_name or not password:
        raise dpapi2_exception.DataError("Username and password are empty")

    if ',' in user_name or '\n' in user_name:
        raise dpapi2_exception.DataError("Username contains invalid characters")

    user_in_db = await UserDB.get_user(user_name=user_name)
    if user_in_db:
        raise dpapi2_exception.IntegrityError(
            f"User {user_name} already exists."
        )
    
    hased_password = hash_password(password)
    user = await UserDB.add_user(user_name=user_name, password=hased_password)
    return user

async def login_user(user_name, password):
    """
    - check with hashed password
    - return jwt
    """
    query = await UserDB.get_user(user_name=user_name)
    if query is None or not verify_password(password, query.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    access_token = create_access_token(data={"sub": user_name})
    refresh_token = create_refresh_token(data={"sub": user_name})
    return Token(access_token=access_token, refresh_token=refresh_token, token_type="bearer")