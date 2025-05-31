import os
import json
import aiofiles
from typing import Any
from server.config.settings import STORAGE_FOLDER, BATCH_SIZE
from server.utils.query_utils import ExpressionNode

import asyncio
from multiprocessing import Pool, cpu_count


def process_rows_batch(args):
    rows_batch, columns, column_types, ast_node = args
    from server.utils.query_utils import ExpressionNode

    def cast(col_type, val):
        if col_type == "integer":
            return int(val)
        elif col_type == "float":
            return float(val)
        elif col_type == "string":
            return val.strip()
        return val

    def evaluate(row, node):
        if node is None:
            return True
        val = node.value

        if node.left is None and node.right is None:
            if isinstance(val, str):
                if val in row:
                    return row[val]
                elif val.replace('.', '', 1).isdigit():
                    return float(val) if '.' in val else int(val)
                return val.strip()
            return val

        if node.right is None:
            return not evaluate(row, node.left)

        left = evaluate(row, node.left)
        right = evaluate(row, node.right)

        match val.upper():
            case "AND":
                return left and right
            case "OR":
                return left or right
            case "=":
                return left == right
            case "<>":
                return left != right
            case "!=":
                return left != right
            case ">":
                return left > right
            case "<":
                return left < right
            case ">=":
                return left >= right
            case "<=":
                return left <= right
            case "+":
                return left + right
            case "-":
                return left - right
            case "*":
                return left * right
            case "/":
                return left / right
            case "%":
                return left % right
            case _:
                raise ValueError(f"Unsupported operator: {val}")

    results = []
    for row in rows_batch:
        casted_row = {col: cast(column_types[col], row[col]) for col in row}
        if ast_node is None or evaluate(casted_row, ast_node):
            if columns == ["*"]:
                results.append(casted_row)
            else:
                results.append({col: casted_row[col] for col in columns})
    return results


class Table:
    def __init__(self, table_name: str, db_name: str, columns_metadata: list[dict[str, Any]]):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, f'{table_name}.csv')
        self.column_metadata = columns_metadata
        self.column_types: dict[str, str] = {
            meta['name']: meta['type'] for meta in self.column_metadata
        }

    async def query(self, columns: list[str], ast: ExpressionNode = None):
        if columns == ["*"]:
            columns = [meta['name'] for meta in self.column_metadata]

        async with aiofiles.open(self.csv_file, 'r') as f:
            header_line = await f.readline()
            headers = [h.strip() for h in header_line.strip().split(',')]

            batch = []
            batch_size = BATCH_SIZE
            pool = Pool(processes = cpu_count())

            async for line in f:
                values = [v.strip() for v in line.strip().split(',')]
                row_dict = dict(zip(headers, values))
                batch.append(row_dict)

                if len(batch) >= batch_size:
                    async for row in self._process_batch(pool, batch, columns, ast):
                        yield row
                    batch.clear()

            if batch:
                async for row in self._process_batch(pool, batch, columns, ast):
                    yield row

            pool.close()
            pool.join()

    async def _process_batch(self, pool, batch: list[dict[str, str]], columns: list[str], ast: ExpressionNode):
        loop = asyncio.get_running_loop()
        args = (batch, columns, self.column_types, ast)
        results = await loop.run_in_executor(None, lambda: process_rows_batch(args))
        for row in results:
            yield json.dumps(row)