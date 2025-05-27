from server.config.settings import STORAGE_FOLDER, BATCH_SIZE
# from server.database.entities.column import Column
# from server.database.entities.row import Row
import time
import os
import json
import csv
from typing import Dict, Any, List

class Table:
    def __init__(self, table_name: str, db_name: str, columns_metedata: List[Dict[str, Any]]):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, f'{table_name}.csv')
        self.column_metadata = columns_metedata
        self.rows = []
        # self._load()

    # def _load(self):
    #     with open(self.csv_file, 'r') as f:
    #         reader = csv.DictReader(f)
    #         for row in reader:
    #             self.rows.append(Row(row))

    

    def filter(self, rows, columns, ast):
        # if columns is None or len(columns) == 0:
        #     columns = [col.name for col in self.columns]
        # rows = []
        # for row in self.rows:
        #     rows.append({col: row[col] for col in columns})
        # # return [
        # #     {col: row[col] for col in columns}
        # #     for row in self.rows
        # # ]
        # 
        return rows
        # return [row.to_dict() for row in rows]

    def query(self, columns: List[str], ast=None) -> List[Dict[str, Any]]:
        if '*' in columns:
            columns = [col.name for col in self.columns_metedata]

        # read the CSV file in batches
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                # filter the row based on the columns
                rows.append(row)
                if len(rows) >= BATCH_SIZE:
                    print(self.filter(rows, columns, ast))
                    yield self.filter(rows, columns, ast)
                    rows = []
            if rows:
                yield self.filter(rows, columns, ast)

        return 
        
    