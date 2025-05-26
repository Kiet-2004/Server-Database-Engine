from server.config.settings import STORAGE_FOLDER
from server.database.entities.column import Column
from server.database.entities.row import Row

import os
import json
import csv
from typing import Dict, Any

class Table:
    def __init__(self, table_name: str, db_name: str):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, table_name, f'{table_name}.csv')
        self.meta_file = os.path.join(STORAGE_FOLDER, db_name, table_name, 'metadata.json')
        self.columns = []
        self.rows = []
        self.num_rows = 0
        self._load()

    def _load(self):
        # load metadata:
        with open(self.meta_file) as f:
            meta_data = json.load(f)
            for column in meta_data['columns']:
                self.columns.append(Column.from_dict(column))
            self.num_rows = meta_data['row_count']
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.rows.append(Row(row))

    def select(self, columns):
        if columns is None or len(columns) == 0:
            columns = [col.name for col in self.columns]
        return [
            {col: row[col] for col in columns}
            for row in self.rows
        ]
        # return [row.to_dict() for row in rows]
    