# Authentication middleware
from fastapi.security import OAuth2PasswordBearer

def authe():
    """
    decode -> payload: id -> raise not auth
    dùng id -> check user -> raise not exitst
    """
    pass