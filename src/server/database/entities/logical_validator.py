from server.database.entities.ast import ExpressionNode
from server.utils.exceptions import dpapi2_exception
 
class LogicalValidator:
    def __init__(self, metadata: dict[str, dict[str, list[dict[str, str]]]]):
        self.metadata = metadata
        self.tables = {}  # key: table_name, value: (db_name, table_metadata)
 
    def _validate_from(self, db_name: str, tables: list[str]):
        if db_name not in self.metadata:
            raise dpapi2_exception.ProgrammingError(f"Database '{db_name}' not found.")
        db_meta = self.metadata[db_name]
 
        for table in tables:
            if table not in db_meta:
                raise dpapi2_exception.ProgrammingError(f"Table '{table}' not found in database '{db_name}'.")
 
            # Convert schema list to dict: {column_name: type}
            schema_list = db_meta[table]
            schema_dict = {col["name"]: col["type"] for col in schema_list}
            self.tables[table] = (db_name, schema_dict)
 
    def _validate_column(self, col: str) -> str:
        parts = col.split('.')
        if len(parts) == 1:
            # Unqualified column: search across all tables in self.tables
            matches = []
            for table, (db, schema) in self.tables.items():
                if parts[0] in schema:
                    matches.append(f"{db}.{table}.{parts[0]}")
            if len(matches) == 0:
                raise dpapi2_exception.ProgrammingError(f"Column '{col}' not found in any table.")
            elif len(matches) > 1:
                raise dpapi2_exception.ProgrammingError(
                    f"Ambiguous column '{col}' found in multiple tables: {matches}"
                )
            return matches[0]
 
        elif len(parts) == 2:
            # Qualified as table.column
            table_name, colname = parts
            if table_name not in self.tables:
                raise dpapi2_exception.ProgrammingError(f"Table '{table_name}' not found in FROM clause.")
            db, schema = self.tables[table_name]
            if colname not in schema:
                raise dpapi2_exception.ProgrammingError(f"Column '{colname}' not found in table '{table_name}'.")
            return f"{db}.{table_name}.{colname}"
 
        elif len(parts) == 3:
            # Fully qualified as db.table.column
            db_name, table_name, colname = parts
            if db_name not in self.metadata:
                raise dpapi2_exception.ProgrammingError(f"Database '{db_name}' not found.")
            if table_name not in self.metadata[db_name]:
                raise dpapi2_exception.ProgrammingError(f"Table '{table_name}' not found in database '{db_name}'.")
            # Check if column exists in schema list
            schema_list = self.metadata[db_name][table_name]
            column_names = [c["name"] for c in schema_list]
            if colname not in column_names:
                raise dpapi2_exception.ProgrammingError(
                    f"Column '{colname}' not found in table '{table_name}' of database '{db_name}'."
                )
            return col  # Already fully qualified

        else:
            raise dpapi2_exception.ProgrammingError(f"Invalid column format: '{col}'")
 
    def _get_column_type(self, full_col: str) -> str:
        db_name, table_name, col = full_col.split(".")
        schema_list = self.metadata[db_name][table_name]
        schema = {c["name"]: c["type"] for c in schema_list}
        if col not in schema:
            raise dpapi2_exception.ProgrammingError(f"Column '{col}' not found in schema of {db_name}.{table_name}")
        return schema[col]
 
    def _validate_condition_ast(self, node: ExpressionNode) -> str:
        if node.left is None and node.right is None:
            # Leaf node: identifier or literal
            if isinstance(node.value, str):
                if '.' in node.value:
                    # If already fully qualified, check existence
                    parts = node.value.split('.')
                    if len(parts) == 3:
                        db_name, table_name, colname = parts
                        schema_list = self.metadata.get(db_name, {}).get(table_name, [])
                        column_names = [c["name"] for c in schema_list]
                        if colname in column_names:
                            return next(
                                c["type"] for c in schema_list if c["name"] == colname
                            )
                # Otherwise, try resolving as column
                try:
                    resolved = self._validate_column(node.value)
                    return self._get_column_type(resolved)
                except dpapi2_exception.ProgrammingError:
                    # If not a column, assume it's a string literal
                    return "string"
            elif isinstance(node.value, (int, float)):
                return "integer" if isinstance(node.value, int) else "float"
            else:
                raise dpapi2_exception.ProgrammingError(f"Unknown literal or identifier: {node.value}")
 
        elif node.value == "NOT":
            operand_type = self._validate_condition_ast(node.left)
            if operand_type != "bool":
                raise dpapi2_exception.ProgrammingError("NOT operator requires boolean operand")
            return "bool"
 
        else:
            left_type = self._validate_condition_ast(node.left)
            right_type = self._validate_condition_ast(node.right)
 
            if node.value in ("AND", "OR"):
                if left_type != "bool" or right_type != "bool":
                    raise dpapi2_exception.ProgrammingError(f"{node.value} requires boolean operands")
                return "bool"
 
            elif node.value in ("=", "!=", "<>", "<", ">", "<=", ">="):
                if left_type != right_type and not (
                    {"integer", "float"} == {left_type, right_type}
                ):
                    raise dpapi2_exception.ProgrammingError(
                        f"Incompatible types in comparison: {left_type} {node.value} {right_type}"
                    )
                return "bool"
 
            elif node.value in ("+", "-", "*", "/", "%"):
                if left_type not in ("integer", "float") or right_type not in ("integer", "float"):
                    raise dpapi2_exception.ProgrammingError(
                        f"Arithmetic operator '{node.value}' requires numeric operands"
                    )
                return "float" if "float" in (left_type, right_type) else "integer"
 
            else:
                raise dpapi2_exception.ProgrammingError(f"Unknown operator: {node.value}")
 
    def validate_logic(self, columns: list[str], table: str, condition_ast: ExpressionNode | None):
        # 1. Parse and validate table (ensure db_name matches metadata)
        parts = table.split(".")
        metadata_dbs = list(self.metadata.keys())
 
        if len(parts) == 2:
            user_db_name, table_name = parts
            if user_db_name not in self.metadata:
                raise dpapi2_exception.ProgrammingError(
                    f"Database '{user_db_name}' not found. Expected one of: {metadata_dbs}"
                )
            db_name = user_db_name
        elif len(parts) == 1:
            if len(self.metadata) != 1:
                raise dpapi2_exception.ProgrammingError(
                    "Ambiguous database. Please specify [db_name].[table_name]."
                )
            db_name = metadata_dbs[0]
            table_name = parts[0]
        else:
            raise dpapi2_exception.ProgrammingError(f"Invalid table format: '{table}'")
 
        # 2. Validate FROM clause
        self._validate_from(db_name, [table_name])
 
        # 3. Normalize columns
        if columns == ["*"]:
            final_columns = ["*"]
        else:
            final_columns = []
            for col in columns:
                full = self._validate_column(col)  # raises if invalid
                final_columns.append(full.split(".")[-1])  # only keep column_name
 
        # 4. Validate and rewrite AST
        def rewrite_ast(node: ExpressionNode | None):
            if node is None:
                return None
            if node.left is None and node.right is None:
                if isinstance(node.value, str):
                    try:
                        resolved = self._validate_column(node.value)
                        node.value = resolved.split(".")[-1]
                    except dpapi2_exception.ProgrammingError:
                        # literal or already validated
                        pass
            else:
                rewrite_ast(node.left)
                rewrite_ast(node.right)
 
        if condition_ast:
            self._validate_condition_ast(condition_ast)
            rewrite_ast(condition_ast)
 
        return final_columns, table_name, condition_ast