# Settings configuration
import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY=os.getenv("SECRET_KEY")
...
...
CSV_FILE = os.getenv("CSV_FILE")

SETTINGS_DIR = os.path.dirname(os.path.abspath(__file__))

SERVER_FOLDER = os.path.dirname(SETTINGS_DIR)

STORAGE_FOLDER = os.path.join(SERVER_FOLDER, 'database/storage')
print(STORAGE_FOLDER)
