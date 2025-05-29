from server.database.entities.table import Table
from server.config.settings import STORAGE_FOLDER
from server.utils.exceptions import dpapi2_exception

from typing import List, Dict
import os
import json

class DB:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.db_path = os.path.join(STORAGE_FOLDER, db_name)


        self.meta_file = os.path.join(STORAGE_FOLDER, db_name, 'metadata.json')
        with open(self.meta_file) as f:
            self.meta_data = json.load(f)

        self.tables: Dict[str, Table] = {}
        for table_name in self.meta_data:
            self._load_table(table_name)

    def _load_table(self, table_name: str):
        table = Table(table_name=table_name, db_name=self.db_name, columns_metadata=self.meta_data[table_name])
        self.tables[table_name] = table


    def get_table(self, table_name: str) -> Table:
        if table_name not in self.tables:
            raise dpapi2_exception.DatabaseError(f"Table {table_name} not found in database {self.db_name}.")
        return self.tables[table_name]


