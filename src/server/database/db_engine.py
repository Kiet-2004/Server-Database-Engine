from typing import List, Dict, Any
import csv
from server.config import settings
import os
class Column:
    def __init__(self, name: str, dtype: str):
        self.name = name
        self.dtype = dtype

class Row:
    def __init__(self, values: Dict):
        self.values = values  # e.g., {"id": 1, "name": "Alice"}

    def __getitem__(self, column_name):
        return self.values.get(column_name)

class Table:
    def __init__(self, name: str, csv_path: str):
        self.name = name
        self.csv_file = csv_path
        self.columns = []
        self.rows = []
        self._load()

    def _load(self):
        csv_path = os.path.join(settings.STORAGE_FOLDER, self.csv_file)
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            self.columns = [Column(name, "str") for name in reader.fieldnames]  # Simple typing
            for row in reader:
                self.rows.append(Row(row))

    def select(self, columns=None):
        if columns is None:
            columns = [col.name for col in self.columns]
        return [
            {col: row[col] for col in columns}
            for row in self.rows
        ]


class DatabaseEngine:
    def __init__(self):
        self.tables = {}

    def load_table(self, name, path):
        self.tables[name] = Table(name, path)

    def query(self, table_name, columns=None):
        return self.tables[table_name].select(columns)


engine = DatabaseEngine()