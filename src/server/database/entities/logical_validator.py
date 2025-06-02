from server.database.entities.ast import AST, ExpressionNode
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
            self.tables[table] = (db_name, schema_dict)  # Use dict instead of list

    def _validate_column(self, col: str) -> str:
        parts = col.split('.')
        if len(parts) == 1:
            matches = []
            for table, (db, schema) in self.tables.items():
                if parts[0] in schema:
                    matches.append(f"{db}.{table}.{parts[0]}")
            if len(matches) == 0:
                raise dpapi2_exception.ProgrammingError(f"Column '{col}' not found in any table.")
            elif len(matches) > 1:
                raise dpapi2_exception.ProgrammingError(f"Ambiguous column '{col}' found in multiple tables: {matches}")
            return matches[0]
        elif len(parts) == 2:
            table, colname = parts
            if table not in self.tables:
                raise dpapi2_exception.ProgrammingError(f"Table '{table}' not found in FROM clause.")
            db, schema = self.tables[table]
            if colname not in schema:
                raise dpapi2_exception.ProgrammingError(f"Column '{colname}' not found in table '{table}'.")
            return f"{db}.{table}.{colname}"
        elif len(parts) == 3:
            db, table, colname = parts
            if db not in self.metadata:
                raise dpapi2_exception.ProgrammingError(f"Database '{db}' not found.")
            if table not in self.metadata[db]:
                raise dpapi2_exception.ProgrammingError(f"Table '{table}' not found in database '{db}'.")
            if colname not in self.metadata[db][table]:
                raise dpapi2_exception.ProgrammingError(f"Column '{colname}' not found in table '{table}' of database '{db}'.")
            return col  # Already fully qualified
        else:
            raise dpapi2_exception.ProgrammingError(f"Invalid column format: '{col}'")

    def _get_column_type(self, full_col: str) -> str:
        db, table, col = full_col.split(".")
        schema_list = self.metadata[db][table]
        schema = {c["name"]: c["type"] for c in schema_list}
        if col not in schema:
            raise dpapi2_exception.ProgrammingError(f"Column '{col}' not found in schema of {db}.{table}")
        return schema[col]

    def _validate_condition_ast(self, node: ExpressionNode) -> str:
        if node.left is None and node.right is None:
            # Leaf node: identifier or literal
            if isinstance(node.value, str):
                if '.' in node.value:
                    for db_name, tables in self.metadata.items():
                        for table_name, schema in tables.items():
                            if node.value in [f"{db_name}.{table_name}.{col}" for col in schema]:
                                return schema[node.value.split('.')[-1]]
                try:
                    resolved = self._validate_column(node.value)
                    return self._get_column_type(resolved)
                except dpapi2_exception.ProgrammingError:
                    return 'string'  # Assume it's a string literal
            elif isinstance(node.value, (int, float)):
                return 'integer' if isinstance(node.value, int) else 'float'
            else:
                raise dpapi2_exception.ProgrammingError(f"Unknown literal or identifier: {node.value}")

        elif node.value == 'NOT':
            operand_type = self._validate_condition_ast(node.left)
            if operand_type != 'bool':
                raise dpapi2_exception.ProgrammingError("NOT operator requires boolean operand")
            return 'bool'

        else:
            left_type = self._validate_condition_ast(node.left)
            right_type = self._validate_condition_ast(node.right)

            if node.value in ('AND', 'OR'):
                if left_type != 'bool' or right_type != 'bool':
                    raise dpapi2_exception.ProgrammingError(f"{node.value} requires boolean operands")
                return 'bool'

            elif node.value in ('=', '!=', '<>', '<', '>', '<=', '>='):
                if left_type != right_type and not (
                    {'integer', 'float'} == {left_type, right_type}
                ):
                    raise dpapi2_exception.ProgrammingError(f"Incompatible types in comparison: {left_type} {node.value} {right_type}")
                return 'bool'

            elif node.value in ('+', '-', '*', '/', '%'):
                if left_type not in ('integer', 'float') or right_type not in ('integer', 'float'):
                    raise dpapi2_exception.ProgrammingError(f"Arithmetic operator '{node.value}' requires numeric operands")
                return 'float' if 'float' in (left_type, right_type) else 'integer'

            else:
                raise dpapi2_exception.ProgrammingError(f"Unknown operator: {node.value}")
            
    def validate_logic(self, columns: list[str], table: str, condition_ast: ExpressionNode | None):
        # 1. Tách db_name và table_name nếu cần
        parts = table.split(".")
        if len(parts) == 2:
            db_name, table_name = parts
        elif len(parts) == 1:
            if len(self.metadata) != 1:
                raise dpapi2_exception.ProgrammingError("Ambiguous database. Please specify [db_name].[table_name].")
            db_name = list(self.metadata.keys())[0]
            table_name = parts[0]
        else:
            raise dpapi2_exception.ProgrammingError(f"Invalid table format: '{table}'")

        # 2. Validate FROM clause
        self._validate_from(db_name, [table_name])

        # 3. Chuẩn hóa columns
        if columns == ["*"]:
            final_columns = ["*"]
        else:
            final_columns = []
            for col in columns:
                full = self._validate_column(col)  # sẽ raise lỗi nếu không hợp lệ
                final_columns.append(full.split(".")[-1])  # chỉ lấy column_name

        # 4. Validate và chuẩn hóa AST
        def rewrite_ast(node: ExpressionNode | None):
            if node is None:
                return None
            if node.left is None and node.right is None:
                if isinstance(node.value, str):
                    try:
                        resolved = self._validate_column(node.value)
                        node.value = resolved.split(".")[-1]  # chỉ lấy column_name
                    except dpapi2_exception.ProgrammingError:
                        pass  # là literal, không cần đổi
            else:
                rewrite_ast(node.left)
                rewrite_ast(node.right)

        if condition_ast:
            self._validate_condition_ast(condition_ast)
            rewrite_ast(condition_ast)

        return final_columns, table_name, condition_ast