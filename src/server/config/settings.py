# Settings configuration
import os
from dotenv import load_dotenv
import secrets
load_dotenv()

# SECRET_KEY=os.getenv("SECRET_KEY")
# ALGORITHM=os.getenv("ALGORITHM")
# ACCESS_TOKEN_EXPIRE_MINUTES=int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
SECRET_KEY= secrets.token_urlsafe(32)
ALGORITHM="HS256"
ACCESS_TOKEN_EXPIRE_MINUTES=25

ACCESS_TOKEN_EXPIRE_MINUTES=1
REFRESH_TOKEN_EXPIRE_DAYS=7

SETTINGS_DIR = os.path.dirname(os.path.abspath(__file__))

SERVER_FOLDER = os.path.dirname(SETTINGS_DIR)

STORAGE_FOLDER = os.path.join(SERVER_FOLDER, 'database/storage')

DB_NAMES = [
   'CompanyDB'
]

BATCH_SIZE = 10
