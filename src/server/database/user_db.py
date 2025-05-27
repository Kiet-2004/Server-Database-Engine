import csv
from pydantic import BaseModel
import os
from server.config import settings

class User(BaseModel):
    user_name: str
    hashed_password: str

class UserDB:
    def __init__(self, data_file = "user.csv") -> None:

        self.data_file = os.path.join(settings.STORAGE_FOLDER, data_file)
        with open(self.data_file, 'r') as file:
            reader = csv.DictReader(file)
            self.data = [User(user_name=row['user_name'], hashed_password=row['password']) for row in reader]

    def get_user(self, user_name: str) -> User | None:
        for user in self.data:
            if user.user_name == user_name:
                return user
        return None
    
    def add_user(self, user_name: str, password: str) -> User | None:
        user = User(user_name=user_name, hashed_password=password)
        self.data.append(user)
        with open(self.data_file, 'a') as file:
            writer = csv.DictWriter(file, fieldnames=["user_name", "hashed_password"])
            writer.writerow({"user_name": user.user_name, "hashed_password": user.hashed_password})
            file.close()
        return user

USER_DATABASE = UserDB()
