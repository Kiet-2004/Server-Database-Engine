from server.config.settings import STORAGE_FOLDER, BATCH_SIZE
import os
import json
import asyncio
import aiofiles
from typing import Dict, Any, List

class Table:
    def __init__(self, table_name: str, db_name: str, columns_metadata: List[Dict[str, Any]]):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, f'{table_name}.csv')

        self.columns_metadata = columns_metadata
    

    def filter(self, rows, columns, ast):
        return rows

    async def query(self, columns: List[str], ast=None):
        if not columns or '*' in columns:
            columns = [col['name'] for col in self.columns_metadata]

        # read the CSV file in batches
        async with aiofiles.open(self.csv_file, 'r') as f:
            # Read the header line first
            header_line = await f.readline()
            headers = [h.strip() for h in header_line.strip().split(',')]
            async for line in f:
                values = [v.strip() for v in line.strip().split(',')]
                row_dict = dict(zip(headers, values))
                await asyncio.sleep(1)
                filtered = self.filter(row_dict, columns, ast)
                yield json.dumps(filtered)
        
    