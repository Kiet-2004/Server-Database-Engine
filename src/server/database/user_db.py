from pydantic import BaseModel
from server.config.settings import USER_DB
from server.utils.exceptions import dpapi2_exception
import os
from typing import Optional
from asyncio import to_thread
import aiofiles
from filelock import FileLock

class User(BaseModel):
    user_name: str
    hashed_password: str

class UserDB:
    def __init__(self, data_file: Optional[str] = None) -> None:
        if not data_file:
            self.data_file = USER_DB
        else:
            self.data_file = data_file
        
        self.lock_file = f"{self.data_file}.lock"

    async def get_user(self, user_name: str) -> Optional[User]:
        try:
            async with aiofiles.open(self.data_file, mode='r') as file:
                header = await file.readline()
                # Skip if file is empty
                if not header.strip():
                    return None

                async for line in file:
                    row = line.strip().split(",")
                    if len(row) < 2:
                        continue
                    name, password = row
                    if name == user_name:
                        return User(user_name=name, hashed_password=password)
        except FileNotFoundError:
            raise dpapi2_exception.OperationalError("User database file not found.")
        except Exception as e:
            raise dpapi2_exception.OperationalError(f"An error occurred while reading the user database: {str(e)}")
        return None
    
    async def add_user(self, user_name: str, password: str) -> User:
        # FileLock is blocking, so we wrap it in a thread executor
        def write_to_file():
            try:
                with FileLock(self.lock_file):
                    file_exists = os.path.exists(self.data_file)
                    write_header = not file_exists or os.path.getsize(self.data_file) == 0
                    with open(self.data_file, mode='a', newline='') as file:
                        if write_header:
                            file.write("user_name,hashed_password\n")
                        file.write(f"{user_name},{password}\n")
            except OSError as e:
                raise dpapi2_exception.OperationalError(f"File write failed: {e}")

        await to_thread(write_to_file)
        return User(user_name=user_name, hashed_password=password)

USER_DATABASE = UserDB()
