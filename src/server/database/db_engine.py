from server.database.entities.db import DB
from server.config.settings import DB_NAMES
from server.utils.exceptions import dpapi2_exception
from server.utils.query_utils import AST, LogicalValidator

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

    def query(self, user_name: str, columns: list[str], table_name: str, ast: AST = None):
        try:
            # Truy cập DB và metadata ngay từ đầu
            db = self.get_db(user_name)
            metadata = db.meta_data

            # Dùng LogicalValidator để xác thực và chuẩn hóa logic truy vấn
            validator = LogicalValidator(metadata)
            columns, table_name, ast = validator.validate_logic(
                columns = columns,
                table = table_name,
                condition_ast = ast
            )

            # Lấy bảng đã được xác thực tên và truy vấn
            table = db.get_table(table_name)
            if not table:
                raise dpapi2_exception.DatabaseError(
                    f"Table '{table_name}' not found in database '{db.db_name}'."
                )

            rows = table.query(columns, ast)
            return rows

        except dpapi2_exception.ProgrammingError as e:
            raise e
        except (
            dpapi2_exception.InterfaceError,
            dpapi2_exception.DatabaseError,
            dpapi2_exception.DataError,
            dpapi2_exception.OperationalError,
            dpapi2_exception.IntegrityError,
            dpapi2_exception.InternalError,
            dpapi2_exception.NotSupportedError,
        ) as e:
            raise e
        except Exception as e:
            raise dpapi2_exception.InternalError(
                f"Unexpected error querying table '{table_name}': {e}"
            ) from e
            
engine = DatabaseEngine()