from server.utils.exceptions import dpapi2_exception

# Represents a node in the expression tree.
# Each node has a value, and optional left and right children for binary operations.
class ExpressionNode:
    def __init__(self, value, left = None, right = None):
        self.value = value
        self.left = left
        self.right = right

    # Provides a string representation of the node for debugging purposes.
    def __repr__(self):
        # If it's a leaf node (no children), just return the value.
        if self.left is None and self.right is None:
            return repr(self.value)
        # If it's a unary operator (like NOT), return (operator operand).
        if self.right is None:
            return f"({self.value} {self.left})"
        # For binary operators, return (left_operand operator right_operand).
        return f"({self.left} {self.value} {self.right})"

# Builds an Abstract Syntax Tree (AST) from a string expression.
class AST:
    def __init__(self, expr: str):
        try:
            # Step 1: Convert the input expression string into a list of tokens.
            self.tokens = self.tokenize(expr)
            # Step 2: Convert the infix tokens into postfix notation (Reverse Polish Notation).
            self.postfix = self.to_postfix(self.tokens)
            # Step 3: Build the expression tree (AST) from the postfix tokens.
            self.root = self.build_tree(self.postfix)
        except ValueError as e:
            # Catch general ValueErrors during parsing and re-raise as a ProgrammingError.
            raise dpapi2_exception.ProgrammingError(f"Syntax error in expression: {e}") from e

    # Converts a string expression into a list of categorized tokens.
    def tokenize(self, expr):
        tokens = []
        i = 0
        while i < len(expr):
            c = expr[i]
            # Skip whitespace characters.
            if c.isspace():
                i += 1
                continue
            # Handle parentheses.
            if c in '()':
                tokens.append((c, c))
                i += 1
            # Handle relational operators (e.g., <=, >=, !=, <>, =).
            elif c in ['<', '>', '!', '=']:
                if c in '<>!' and i + 1 < len(expr) and expr[i + 1] == '=':
                    tokens.append(('OP', expr[i:i + 2]))
                    i += 2
                elif c == '<' and i + 1 < len(expr) and expr[i + 1] == '>':
                    tokens.append(('OP', '<>'))
                    i += 2
                else:
                    tokens.append(('OP', c))
                    i += 1
            # Handle arithmetic operators.
            elif c in '+-*/%':
                tokens.append(('OP', c))
                i += 1
            # Handle single-quoted string literals.
            elif c == "'":
                j = i + 1
                while j < len(expr) and expr[j] != "'":
                    j += 1
                if j >= len(expr):
                    # Raise an error if a string literal is not closed.
                    raise dpapi2_exception.ProgrammingError("Unterminated string literal")
                tokens.append(('STRING', expr[i + 1:j]))
                i = j + 1
            # Handle double-quoted string literals.
            elif c == '"':
                j = i + 1
                while j < len(expr) and expr[j] != '"':
                    j += 1
                if j >= len(expr):
                    # Raise an error if a string literal is not closed.
                    raise dpapi2_exception.ProgrammingError("Unterminated string literal")
                tokens.append(('STRING', expr[i + 1:j]))
                i = j + 1
            # Handle numeric literals (integers and floats).
            elif c.isdigit():
                j = i
                dot_seen = False
                while j < len(expr):
                    if expr[j].isdigit():
                        j += 1
                    elif expr[j] == '.' and not dot_seen:
                        dot_seen = True
                        j += 1
                    else:
                        break
                num_str = expr[i:j]
                try:
                    tokens.append(('NUMBER', float(num_str) if dot_seen else int(num_str)))
                except ValueError:
                    # Raise an error for invalid numeric formats.
                    raise dpapi2_exception.DataError(f"Invalid numeric literal: {num_str}")
                i = j
            # Handle identifiers (column names, function names) and logical keywords.
            elif c.isalpha() or c in ['_', '.', '-']:
                j = i
                while j < len(expr) and (expr[j].isalnum() or expr[j] in ['_', '.', '-']):
                    j += 1
                word = expr[i:j]
                upper_word = word.upper()
                # Check for logical keywords.
                if upper_word in ['AND', 'OR', 'NOT']:
                    tokens.append((upper_word, upper_word))
                else:
                    # Otherwise, it's an identifier.
                    tokens.append(('ID', word))
                i = j
            # Raise an error for any unrecognized characters.
            else:
                raise dpapi2_exception.ProgrammingError(f"Invalid character '{c}' in expression")
        return tokens

    # Converts a list of infix tokens to postfix (Reverse Polish Notation) using the Shunting-Yard algorithm.
    def to_postfix(self, tokens):
        # Define operator precedence. Higher numbers mean higher precedence.
        precedence = {
            'OR': 1,
            'AND': 2,
            'NOT': 3,
            '=': 4, '!=': 4, '<>': 4, '<': 4, '>': 4, '<=': 4, '>=': 4,
            '+': 5, '-': 5,
            '*': 6, '/': 6, '%': 6
        }
        output = []  # Stores the postfix expression.
        stack = []   # Operator stack.

        for kind, val in tokens:
            if kind in ('ID', 'NUMBER', 'STRING'):
                # Operands are added directly to the output.
                output.append((kind, val))
            elif kind in ('OP', 'AND', 'OR', 'NOT'):
                # Operators: Pop operators from stack to output if their precedence is higher or equal.
                while stack and stack[-1][0] != '(' and precedence.get(stack[-1][1], 0) >= precedence[val]:
                    output.append(stack.pop())
                stack.append((kind, val))
            elif kind == '(':
                # Left parenthesis: Push onto the stack.
                stack.append((kind, val))
            elif kind == ')':
                # Right parenthesis: Pop operators from stack to output until a left parenthesis is found.
                while stack and stack[-1][0] != '(':
                    output.append(stack.pop())
                if not stack:
                    # Error if no matching left parenthesis.
                    raise dpapi2_exception.ProgrammingError("Mismatched parentheses")
                stack.pop() # Pop the left parenthesis.
        # After processing all tokens, pop any remaining operators from the stack to the output.
        while stack:
            if stack[-1][0] == '(':
                # Error if any left parenthesis remains.
                raise dpapi2_exception.ProgrammingError("Mismatched parentheses")
            output.append(stack.pop())
        return output

    # Builds an Expression Tree (AST) from a list of postfix tokens.
    def build_tree(self, postfix_tokens):
        stack = []
        for kind, value in postfix_tokens:
            if kind in ('ID', 'NUMBER', 'STRING'):
                # Operands become leaf nodes in the tree.
                stack.append(ExpressionNode(value))
            elif value == 'NOT':
                # 'NOT' is a unary operator, it needs one operand.
                if not stack:
                    raise dpapi2_exception.ProgrammingError("Missing operand for NOT")
                operand = stack.pop()
                stack.append(ExpressionNode(value, operand, None)) # Left child is the operand, right is None.
            else:
                # Binary operators need two operands.
                if len(stack) < 2:
                    raise dpapi2_exception.ProgrammingError(f"Missing operand for operator '{value}'")
                right = stack.pop()  # Right operand is popped first.
                left = stack.pop()   # Left operand is popped second.
                stack.append(ExpressionNode(value, left, right)) # Create a new node with operator and its children.
        if len(stack) != 1:
            # After building, there should be exactly one node left, which is the root of the tree.
            raise dpapi2_exception.ProgrammingError("Invalid expression")
        return stack[0]

# SQL Parser responsible for dissecting SQL SELECT queries.
class SQLParser:
    def parse_query(self, query: str):
        # Remove leading/trailing whitespace and semicolons.
        query = query.strip(' ;')

        # Normalize query: remove newlines/tabs and extra spaces
        query = ' '.join(query.replace('\n', ' ').replace('\t', ' ').split())

        try:
            # Run query validation
            self._validate_query(query)
        except Exception as e:
            raise dpapi2_exception.ProgrammingError(f"Invalid query: {e}") from e

        try:
            # Find the starting positions of key SQL keywords.
            keyword_positions = self._find_keyword_positions(query, ["SELECT", "FROM", "WHERE"])
        except Exception as e:
            raise dpapi2_exception.ProgrammingError("Error parsing SQL keywords") from e

        # Ensure essential clauses are present.
        if 'SELECT' not in keyword_positions or 'FROM' not in keyword_positions:
            raise dpapi2_exception.ProgrammingError("Missing SELECT or FROM clause")

        # Extract the start indices for each clause.
        select_start = keyword_positions['SELECT']
        from_start = keyword_positions['FROM']
        where_start = keyword_positions.get('WHERE') # WHERE clause is optional.

        # Extract the column names.
        column_str = query[select_start + len("SELECT"):from_start].strip()
        if not column_str:
            raise dpapi2_exception.ProgrammingError("Missing column names after SELECT")
        columns = ["*"] if column_str == "*" else [col.strip() for col in column_str.split(",")]

        # Extract the FROM and WHERE clauses.
        if where_start:
            from_clause = query[from_start + len("FROM"):where_start].strip()
            where_clause = query[where_start + len("WHERE"):].strip()
        else:
            from_clause = query[from_start + len("FROM"):].strip()
            where_clause = None

        if not from_clause:
            raise dpapi2_exception.ProgrammingError("Missing table name in FROM clause")

        # Extract table names.
        table_names = [name.strip() for name in from_clause.split(",")]

        return {
            "columns": columns,
            "tables": table_names,
            # Build an AST for the WHERE clause condition if it exists.
            "condition_ast": AST(where_clause).root if where_clause else None
        }

    # Helper method to find the starting character positions of keywords in the query string.
    def _find_keyword_positions(self, query: str, keywords: list[str]) -> dict[str, int]:
        words = query.split() # Split the query into words.
        positions = {}
        word_index = 0
        kw_index = 0
        sum_pos = 0 # Keeps track of the cumulative position in the original string.
        while word_index < len(words) and kw_index < len(keywords):
            if keywords[kw_index].upper() == words[word_index].upper():
                positions[keywords[kw_index]] = sum_pos # Store the starting position of the keyword.
                kw_index += 1
            sum_pos += len(words[word_index]) + 1 # Add length of current word and a space.
            word_index += 1
        return positions

    def _validate_query(self, query: str):
        # Check for semicolon outside of string literals
        in_string = False
        escaped = False
        for i, char in enumerate(query):
            if char == "'" and not escaped:
                in_string = not in_string
            elif char == "\\":
                escaped = not escaped
                continue
            elif char == ";" and not in_string:
                remaining = query[i + 1:].strip()
                if remaining:
                    raise dpapi2_exception.NotSupportedError("Multiple SQL statements are not supported")
            escaped = False

        # Check for unsupported keywords (outside of string literals)
        unsupported_keywords = [
            "group by", "order by", "having", "limit", "offset",
            "join", "left join", "right join", "inner join", "outer join",
            "union", "intersect", "except",
            "insert", "update", "delete", "create", "drop", "alter",
            "in(", "between", "like", "is null", "exists",
            "distinct", "top", "into", " as ",
            "count(", "min(", "max(", "sum(", "avg("
        ]

        # Scan outside of string literals
        lower = query.lower()
        i = 0
        in_string = False
        while i < len(lower):
            if lower[i] == "'":
                in_string = not in_string
                i += 1
                continue
            if not in_string:
                for keyword in unsupported_keywords:
                    if lower.startswith(keyword, i):
                        raise dpapi2_exception.NotSupportedError(f"Unsupported keyword or function: {keyword.strip()}")
            i += 1
            
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