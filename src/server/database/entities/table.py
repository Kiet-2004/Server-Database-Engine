from server.config.settings import STORAGE_FOLDER
from server.database.entities.column import Column
from server.database.entities.row import Row

import os
import json
import csv
from typing import Dict, Any, List

class Table:
    def __init__(self, table_name: str, db_name: str, columns: List[Dict]):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, f'{table_name}.csv')
        self.columns = [Column.from_dict(column) for column in columns]
        self.rows = []
        self._load()

    def _load(self):
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.rows.append(Row(row))

    def select(self, columns):
        if columns is None or len(columns) == 0:
            columns = [col.name for col in self.columns]
        rows = []
        for row in self.rows:
            rows.append({col: row[col] for col in columns})
        # return [
        #     {col: row[col] for col in columns}
        #     for row in self.rows
        # ]
        return rows
        # return [row.to_dict() for row in rows]
    