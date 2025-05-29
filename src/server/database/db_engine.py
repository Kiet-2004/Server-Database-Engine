from server.database.entities.db import DB
from server.config.settings import DB_NAMES
from server.utils.exceptions import dpapi2_exception

from typing import List, Dict, Any



class DatabaseEngine:
    def __init__(self):
        self.db_pool: Dict[str, DB] = {}
        # load all databases
        for db_name in DB_NAMES:
            db = DB(db_name)
            self.db_pool[db_name] = db

        self.user_db: Dict[str, str] = {}



    def load_db(self, user_name: str, db_name: str):
        # check db_name is in DB_NAMES
        if db_name not in self.db_pool:
            raise dpapi2_exception.DatabaseError(f"Database {db_name} not found.")
        
        # check user_name is in user_db
        if user_name in self.user_db:
            raise dpapi2_exception.DatabaseError(f"User {user_name} already connected to a database.")
        
        # load the database
        self.user_db[user_name] = db_name

    def get_db(self, user_name: str) -> DB:
        
        if user_name not in self.user_db:
            raise dpapi2_exception.DatabaseError(f"User {user_name} not connected to any database.")
        
        db_name = self.user_db[user_name]
        return self.db_pool[db_name]


    def query(self, user_name: str, table_name: str, columns=None, ast=None):
        db = self.get_db(user_name)
        table = db.get_table(table_name)

        if not table:
            raise dpapi2_exception.DatabaseError(
                f"Table {table_name} not found in database {db.db_name}."
            )

        return table.query(columns=columns, ast=ast)
        


engine = DatabaseEngine()
