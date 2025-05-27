import csv
import os
from parse import SQLParser, ExpressionNode
from typing import Any

class SQLHandler:
    def __init__(self, csv_dir: str, valid_tables: set[str], batch_size: int = 128):
        self.csv_dir = csv_dir
        self.valid_tables = valid_tables
        self.batch_size = batch_size
        self.table_columns = {}  # Cache for column names per table

    def try_cast(self, value: str):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value.strip()  # Clean extra spaces if any

    def load_table_columns(self, table: str):
        if table in self.table_columns:
            return self.table_columns[table]
        
        path = os.path.join(self.csv_dir, f"{table}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file for table '{table}' not found at {path}")
        
        with open(path, newline = '', encoding = 'utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            self.table_columns[table] = headers
            return headers

    def resolve_column(self, col: str, tables: list[str]) -> str:
        """Disambiguate a column name."""
        if '.' in col:
            parts = col.split('.')
            if len(parts) == 2:
                table, column = parts
            elif len(parts) == 3:
                _, table, column = parts
            else:
                raise ValueError(f"Invalid column reference: {col}")
            
            if table not in tables:
                raise ValueError(f"Unknown table reference in column: {col}")
            if column not in self.load_table_columns(table):
                raise ValueError(f"Column '{column}' not found in table '{table}'")
            return f"{table}.{column}"
        else:
            found_in = []
            for table in tables:
                if col in self.load_table_columns(table):
                    found_in.append(table)
            if len(found_in) == 0:
                raise ValueError(f"Column '{col}' not found in any selected table")
            elif len(found_in) > 1:
                raise ValueError(f"Ambiguous column '{col}', found in tables: {', '.join(found_in)}")
            return f"{found_in[0]}.{col}"

    def check_ast_identifiers(self, node: ExpressionNode, tables: list[str]):
        if node is None:
            return
        if isinstance(node.value, str) and node.left is None and node.right is None:
            try:
                self.resolve_column(node.value, tables)
            except ValueError as e:
                raise ValueError(f"Invalid identifier in WHERE clause: {e}")
        self.check_ast_identifiers(node.left, tables)
        self.check_ast_identifiers(node.right, tables)

    def evaluate_condition(self, row: dict[str, Any], ast_node: ExpressionNode, tables: list[str]):
        if ast_node is None:
            return True
        val = ast_node.value

        if ast_node.left is None and ast_node.right is None:
            if isinstance(val, str):
                if val.replace('.', '', 1).isdigit():  # crude number check
                    return self.try_cast(val)
                try:
                    resolved = self.resolve_column(val, tables)
                    return row.get(resolved, val)
                except ValueError:
                    return val.strip()
            return val

        # Unary (e.g., NOT)
        if ast_node.right is None:
            return not self.evaluate_condition(row, ast_node.left, tables)

        left = self.evaluate_condition(row, ast_node.left, tables)
        right = self.evaluate_condition(row, ast_node.right, tables)

        # Binary operator
        if val == "AND":
            return left and right
        if val == "OR":
            return left or right
        if val == "=":
            return left == right
        if val in ("<>", "!="):
            return left != right
        if val == ">":
            return float(left) > float(right)
        if val == "<":
            return float(left) < float(right)
        if val == ">=":
            return float(left) >= float(right)
        if val == "<=":
            return float(left) <= float(right)
        if val == "+":
            return float(left) + float(right)
        if val == "-":
            return float(left) - float(right)
        if val == "*":
            return float(left) * float(right)
        if val == "/":
            return float(left) / float(right)
        if val == "%":
            return float(left) % float(right)

        raise ValueError(f"Unsupported operator: {val}")

    def collect(self, query: str):
        parser = SQLParser()
        parsed = parser.parse_query(query)
        columns = parsed["columns"]
        tables = parsed["tables"]
        cond_ast = parsed["condition_ast"]

        # Validate tables
        for table in tables:
            if table not in self.valid_tables:
                raise ValueError(f"Unknown table: {table}")

        # Validate column references
        resolved_columns = []
        if columns != ["*"]:
            for col in columns:
                resolved_columns.append(self.resolve_column(col, tables))

        self.check_ast_identifiers(cond_ast, tables)

        # Load CSVs one by one, join logic may be added later
        batch = []
        for table in tables:
            path = os.path.join(self.csv_dir, f"{table}.csv")
            with open(path, newline = '', encoding = 'utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    clean_row = {
                        f"{table}.{k}": self.try_cast(v)
                        for k, v in row.items()
                    }
                    if cond_ast is None or self.evaluate_condition(clean_row, cond_ast, tables):
                        if columns == ["*"]:
                            batch.append(clean_row)
                        else:
                            batch.append({col: clean_row[col] for col in resolved_columns})
                        if len(batch) >= self.batch_size:
                            yield batch
                            batch = []
        if batch:
            yield batch


handler = SQLHandler(csv_dir = "", valid_tables = {"users"})

query = "SELECT col_2, col_4 FROM users WHERE col_4 * 2 >= col_10 + 5"
for batch in handler.collect(query):
    for row in batch:
        print(row)
    
    break

parser = SQLParser()
parsed = parser.parse_query(query)
columns = parsed["columns"]
tables = parsed["tables"]
cond_ast = parsed["condition_ast"]

print(columns)
print(tables)
print(cond_ast)