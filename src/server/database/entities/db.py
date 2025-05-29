from server.database.entities.table import Table
from server.config.settings import STORAGE_FOLDER
from server.utils.exceptions import dpapi2_exception
import os
import json

class DB:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.db_path = os.path.join(STORAGE_FOLDER, db_name)
        self.meta_file = os.path.join(self.db_path, 'metadata.json')
        self.tables: dict[str, Table] = {}

        if not os.path.exists(self.db_path):
            raise dpapi2_exception.DatabaseError(f"Database folder '{self.db_path}' does not exist.")

        self.load_db()

    def load_db(self):
        try:
            with open(self.meta_file, "r", encoding="utf-8") as f:
                self.meta_data = json.load(f)
        except FileNotFoundError:
            raise dpapi2_exception.DatabaseError(f"Metadata file not found for database '{self.db_name}'.")
        except json.JSONDecodeError as e:
            raise dpapi2_exception.DatabaseError(f"Invalid metadata file format in '{self.meta_file}': {e}")
        except Exception as e:
            raise dpapi2_exception.InternalError(f"Unexpected error loading metadata: {e}") from e

        for table_name in self.meta_data[self.db_name]:
            try:
                self.load_table(table_name)
            except Exception as e:
                raise dpapi2_exception.DatabaseError(f"Failed to load table '{table_name}' from DB '{self.db_name}': {e}") from e

    def load_table(self, table_name: str):
        try:
            columns_metadata = self.meta_data[self.db_name][table_name]
        except KeyError:
            raise dpapi2_exception.DatabaseError(f"Metadata for table '{table_name}' not found.")

        try:
            table = Table(
                table_name = table_name,
                db_name = self.db_name,
                columns_metadata = columns_metadata
            )
            self.tables[table_name] = table
        except Exception as e:
            raise dpapi2_exception.InternalError(f"Failed to initialize Table object for '{table_name}': {e}") from e

    def get_table(self, table_name: str) -> Table:
        if table_name not in self.tables:
            raise dpapi2_exception.DatabaseError(f"Table '{table_name}' not found in database '{self.db_name}'.")
        return self.tables[table_name]