import csv
from pydantic import BaseModel
import os
from server.config import settings

class UserDB:
    def __init__(self, data_file = "user.csv") -> None:

        self.data_file = os.path.join(settings.STORAGE_FOLDER, data_file)
        with open(self.data_file, 'r') as file:
            reader = csv.DictReader(file)
            self.data = [{"user_name": row['user_name'], "password": row['password']} for row in reader]

    def get_user(self, user_name: str) -> str | None:
        for user in self.data:
            if user['user_name'] == user_name:
                return user['user_name']
        return None
    
    def add_user(self, user_name: str, password: str) -> str | None:
        user = {"user_name": user_name, "password": password}
        self.data.append(user)
        with open(self.data_file, 'a') as file:
            writer = csv.DictWriter(file, fieldnames=["user_name", "password"])
            writer.writerow(user)
            file.close()
        return user['user_name']

USER_DATABASE = UserDB()
