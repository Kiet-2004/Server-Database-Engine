from server.config.settings import STORAGE_FOLDER
import os
import json
import aiofiles
from typing import Any
from server.database.entities.ast import ExpressionNode
from server.utils.exceptions import dpapi2_exception
class Table:
    def __init__(self, table_name: str, db_name: str, columns_metadata: list[dict[str, Any]]):
        self.name = table_name
        self.csv_file = os.path.join(STORAGE_FOLDER, db_name, f'{table_name}.csv')
        self.column_metadata = columns_metadata
        self.rows = []
 
        # Tạo dict ánh xạ tên cột đơn giản -> kiểu
        self.column_types: dict[str, str] = {
            meta['name']: meta['type'] for meta in self.column_metadata
        }
 
    def cast(self, column: str, value: str) -> Any:
        """Ép kiểu theo metadata (column là tên cột đơn giản)."""
        col_type = self.column_types.get(column)
        if col_type == "integer":
            return int(value)
        elif col_type == "float":
            return float(value)
        elif col_type == "string":
            return value.strip()
        else:
            raise ValueError(f"Unsupported type '{col_type}' for column '{column}'")
 
    def evaluate_condition(self, row: dict[str, Any], ast_node: ExpressionNode) -> Any:
        if ast_node is None:
            return True
 
        val = ast_node.value
 
        if ast_node.left is None and ast_node.right is None:
            # Leaf node: column name or constant
            if isinstance(val, str):
                if val in row:
                    return row[val]
                elif val.replace('.', '', 1).isdigit():
                    return float(val) if '.' in val else int(val)
                return val.strip()
            return val
 
        if ast_node.right is None:  # Unary operator, e.g., NOT
            return not self.evaluate_condition(row, ast_node.left)
 
        left = self.evaluate_condition(row, ast_node.left)
        right = self.evaluate_condition(row, ast_node.right)
 
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
 
    def filter(self, row: dict[str, Any], columns: list[str], ast: ExpressionNode | None) ->dict[str, Any] | None:
        casted_row = {
            col: self.cast(col, val) for col, val in row.items()
        }
 
        if ast is None or self.evaluate_condition(casted_row, ast):
            if columns == ["*"]:
                return casted_row
            else:
                return {col: casted_row[col] for col in columns}
        else:
            return None


    async def query(self, columns: list[str], ast=None):
        if columns == ["*"]:
            columns = [meta['name'] for meta in self.column_metadata]

        # read the CSV file in batches
        try:
            async with aiofiles.open(self.csv_file, 'r') as f:
                # Read the header line first
                header_line = await f.readline()
                headers = [h.strip() for h in header_line.strip().split(',')]
                async for line in f:
                    values = [v.strip() for v in line.strip().split(',')]
                    row_dict = dict(zip(headers, values))
                    # await asyncio.sleep(1)

                    filtered = self.filter(row_dict, columns, ast)
                    if filtered is not None:
                        yield json.dumps(filtered)
        except FileNotFoundError:
            raise dpapi2_exception.OperationalError(f"File '{self.csv_file}' not found for table '{self.name}'.")
        except Exception as e:
            raise dpapi2_exception.InternalError(f"Unexpected error querying table '{self.name}': {e}") from e