from server.database.entities.db import DB
from server.config.settings import DB_NAMES
from server.utils.exceptions import dpapi2_exception
from server.database.entities.ast import AST

class DatabaseEngine:
    def __init__(self):
        self.db_pool: dict[str, DB] = {}
        self.user_db: dict[str, str] = {}

        for db_name in DB_NAMES:
            try:
                db = DB(db_name)
                self.db_pool[db_name] = db
            except Exception as e:
                raise dpapi2_exception.DatabaseError(f"Failed to load database '{db_name}': {e}") from e

    def load_db(self, user_name: str, db_name: str):
        if db_name not in self.db_pool:
            raise dpapi2_exception.DatabaseError(f"Database '{db_name}' not found.")

        if user_name in self.user_db:
            raise dpapi2_exception.DatabaseError(f"User '{user_name}' already connected to a database.")

        self.user_db[user_name] = db_name

    def get_db(self, user_name: str) -> DB:
        if user_name not in self.user_db:
            raise dpapi2_exception.DatabaseError(f"User '{user_name}' not connected to any database.")

        db_name = self.user_db[user_name]
        if db_name not in self.db_pool:
            raise dpapi2_exception.InternalError(f"Database '{db_name}' not loaded in memory.")
        
        return self.db_pool[db_name]
    
    def disconnect_user(self, user_name: str):
        """
        Disconnect a user from the database by removing their connection.
        """
        if user_name in self.user_db:
            del self.user_db[user_name]
            return {"message": f"User {user_name} disconnected successfully."}
        else:
            raise dpapi2_exception.DatabaseError(f"User {user_name} is not connected to any database.")
    
    def get_metadata(self, user_name: str):
        """
        Get metadata for the database the user is connected to.
        """

        db = self.get_db(user_name)
        return db.meta_data


    def query_execute(self, db_name: str, columns: list[str], table_name: str, ast: AST = None):

        # Lấy bảng đã được xác thực tên và truy vấn
        db = self.db_pool.get(db_name)
        table = db.get_table(table_name)

        rows = table.select(columns, ast)
        return rows

            
engine = DatabaseEngine()