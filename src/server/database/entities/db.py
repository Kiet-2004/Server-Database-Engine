from server.database.entities.table import Table
from server.config.settings import STORAGE_FOLDER

from typing import List
import os
import json

class DB:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.db_path = os.path.join(STORAGE_FOLDER, db_name)
        self.meta_file = os.path.join(STORAGE_FOLDER, db_name, 'metadata.json')
        self.tables = {}
        self.load_db()

    def load_db(self):
        # load metadata:
        with open(self.meta_file) as f:
            self.meta_data = json.load(f)
            for table in self.meta_data['tables']:
                self.load_table(table['name'], columns=table['columns'])

    def load_table(self, table_name, columns=None):
        table = Table(table_name=table_name, db_name=self.db_name, columns=columns)
        self.tables[table_name] = table



    def get_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table


