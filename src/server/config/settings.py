# Settings configuration
import os
from dotenv import load_dotenv

load_dotenv()

# SECRET_KEY=os.getenv("SECRET_KEY")
# ALGORITHM=os.getenv("ALGORITHM")
# ACCESS_TOKEN_EXPIRE_MINUTES=int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
SECRET_KEY="abc"
ALGORITHM="HS256"
ACCESS_TOKEN_EXPIRE_MINUTES=5
...
