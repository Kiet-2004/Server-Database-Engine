from server.database.entities.table import Table
from server.config.settings import STORAGE_FOLDER

from typing import List
import os

class DB:
    def __init__(self, db_name):
        self.db_name = db_name
        self.db_path = os.path.join(STORAGE_FOLDER, db_name)
        self.tables = {}
        self.load_db()

    def load_table(self, table_name):
        table = Table(table_name=table_name, db_name=self.db_name)
        self.tables[table_name] = table

    def load_db(self):
        for table_name in os.listdir(self.db_path):
            self.load_table(table_name)

    def get_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table


