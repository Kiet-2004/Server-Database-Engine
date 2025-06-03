from src.server.utils.exceptions import dpapi2_exception
from src.server.database.entities.ast import AST
import re

class SQLParser:
    def parse_query(self, query: str):
        # 1. Remove leading/trailing whitespace and semicolons.
        if query is None:
            # None is not valid SQL
            raise dpapi2_exception.InterfaceError("Query cannot be None")
        query = query.strip(' ;')
        if not query:
            raise dpapi2_exception.ProgrammingError("Empty query is not allowed")

        # 2. Normalize query: remove newlines/tabs and extra spaces
        query = ' '.join(query.replace('\n', ' ').replace('\t', ' ').split())

        # 3. Check for multiple statements and unsupported characters/keywords
        try:
            self._validate_query(query)
        except dpapi2_exception.Error:
            # Let any dbapi2 exception subclass (ProgrammingError or NotSupportedError) propagate
            raise
        except Exception as e:
            # Wrap any unexpected error into ProgrammingError
            raise dpapi2_exception.ProgrammingError(f"Invalid query validation: {e}") from e

        # 4. Find the starting positions of key SQL keywords.
        try:
            keyword_positions = self._find_keyword_positions(query, ["SELECT", "FROM", "WHERE"])
        except dpapi2_exception.ProgrammingError:
            # Let ProgrammingError propagate
            raise
        except Exception as e:
            raise dpapi2_exception.ProgrammingError(f"Error parsing SQL keywords: {e}") from e

        # 5. Ensure essential clauses are present.
        if 'SELECT' not in keyword_positions or 'FROM' not in keyword_positions:
            raise dpapi2_exception.ProgrammingError("Missing SELECT or FROM clause")

        select_start = keyword_positions['SELECT']
        from_start = keyword_positions['FROM']
        where_start = keyword_positions.get('WHERE')  # WHERE clause is optional.

        # 6. Check keyword ordering: SELECT < FROM < WHERE (if WHERE exists)
        if not (select_start < from_start):
            raise dpapi2_exception.ProgrammingError("SELECT must come before FROM")
        if where_start is not None and not (from_start < where_start):
            raise dpapi2_exception.ProgrammingError("FROM must come before WHERE")

        # 7. Extract the column names substring.
        column_str = query[select_start + len("SELECT"):from_start].strip()
        if not column_str:
            raise dpapi2_exception.ProgrammingError("Missing column list after SELECT")

        # 8. Check for wildcard '*' rules and parse columns.
        #    - Only "*" alone is valid; không cho "* , col" hay "col, *"
        columns = []
        if column_str == "*":
            columns = ["*"]
        else:
            # Split by commas and strip spaces
            raw_cols = [col.strip() for col in column_str.split(",")]
            if any(not col for col in raw_cols):
                raise dpapi2_exception.ProgrammingError("Invalid comma placement in column list")
            if "*" in raw_cols:
                raise dpapi2_exception.ProgrammingError("Wildcard '*' must be alone if used")

            # Validate each column identifier (allow 1-, 2- hoặc 3-part)
            for col in raw_cols:
                if not self._is_valid_column_name(col):
                    raise dpapi2_exception.ProgrammingError(f"Invalid column name: '{col}'")
            columns = raw_cols

        # 9. Extract FROM and WHERE clause strings
        if where_start is not None:
            from_clause = query[from_start + len("FROM"):where_start].strip()
            where_clause = query[where_start + len("WHERE"):].strip()
        else:
            from_clause = query[from_start + len("FROM"):].strip()
            where_clause = None

        if not from_clause:
            raise dpapi2_exception.ProgrammingError("Missing table name(s) in FROM clause")

        # 10. Parse table names (support multi-table with commas)
        raw_tables = [tbl.strip() for tbl in from_clause.split(",")]
        if any(not tbl for tbl in raw_tables):
            raise dpapi2_exception.ProgrammingError("Invalid comma placement in FROM clause")
        table_names = []
        for tbl in raw_tables:
            # Only allow simple identifiers (no schema.table.column, no aliases, no JOIN)
            if not self._is_valid_identifier(tbl):
                raise dpapi2_exception.ProgrammingError(f"Invalid table name: '{tbl}'")
            table_names.append(tbl)

        # 11. Build AST for WHERE clause if exists
        condition_ast = None
        if where_clause:
            if not where_clause:
                raise dpapi2_exception.ProgrammingError("Empty WHERE clause after WHERE keyword")
            # Try to build AST, nếu AST constructor ném lỗi (ví dụ: cú pháp sai), catch lại
            try:
                condition_ast = AST(where_clause).root
            except dpapi2_exception.Error:
                # Nếu AST ném dbapi2 exception, propagate
                raise
            except Exception as e:
                raise dpapi2_exception.ProgrammingError(f"Invalid WHERE condition: {e}") from e

        return {
            "columns": columns,
            "tables": table_names,
            "condition_ast": condition_ast
        }

    def _find_keyword_positions(self, query: str, keywords: list[str]) -> dict[str, int]:
        """
        Find the first (and only) occurrence of each keyword in order.
        Raise ProgrammingError nếu:
        - Một keyword bị lặp.
        - Thiếu keyword.
        - Hoặc thứ tự keyword không đúng.
        """
        tokens = query.split()  # splitting on whitespace
        positions: dict[str, int] = {}
        sum_pos = 0  # cumulative index in original string
        kw_index = 0

        for token in tokens:
            upper_tok = token.upper()
            stripped = re.sub(r"[^\w]", "", upper_tok)

            if kw_index < len(keywords) and stripped == keywords[kw_index]:
                key = keywords[kw_index]
                if key in positions:
                    raise dpapi2_exception.ProgrammingError(f"Keyword '{key}' appears multiple times")
                positions[key] = sum_pos
                kw_index += 1

            sum_pos += len(token) + 1  # token length + 1 for the space

        if "SELECT" not in positions or "FROM" not in positions:
            raise dpapi2_exception.ProgrammingError("Missing SELECT or FROM keyword")
        return positions

    def _validate_query(self, query: str):
        """
        Validate query for:
        - Multiple statements (disallow ';' ngoài literal).
        - Unsupported keywords/functions (ngoài danh sách).
        """
        # 1. Check for semicolon outside of string literals
        in_string = False
        escaped = False
        for idx, char in enumerate(query):
            if char == "\\" and not escaped:
                escaped = True
                continue
            if char == "'" and not escaped:
                in_string = not in_string
            elif char == ";" and not in_string:
                remaining = query[idx + 1:].strip()
                if remaining:
                    raise dpapi2_exception.NotSupportedError("Multiple SQL statements are not supported")
            escaped = False

        # 2. Check for unsupported keywords/functions outside string literals
        unsupported = [
            "group by", "order by", "having", "limit", "offset",
            "join", "left join", "right join", "inner join", "outer join",
            "union", "intersect", "except",
            "insert", "update", "delete", "create", "drop", "alter",
            "in(", "between ", "like ", "is null", "exists ",
            "distinct", "top ", "into ",
            "count(", "min(", "max(", "sum(", "avg("
        ]
        lower = query.lower()
        i = 0
        in_string = False
        while i < len(lower):
            if lower[i] == "\\":
                i += 2
                continue
            if lower[i] == "'" and not in_string:
                in_string = True
                i += 1
                continue
            elif lower[i] == "'" and in_string:
                in_string = False
                i += 1
                continue

            if not in_string:
                for kw in unsupported:
                    if lower.startswith(kw, i):
                        raise dpapi2_exception.NotSupportedError(
                            f"Unsupported keyword or function: '{kw.strip()}'"
                        )
            i += 1

    def _is_valid_identifier(self, name: str) -> bool:
        """
        Kiểm tra xem tên có phải là một identifier SQL hợp lệ đơn giản không:
        - Bắt đầu bằng chữ (a-z hoặc A-Z) hoặc gạch dưới (_).
        - Các ký tự tiếp theo có thể là chữ, số hoặc gạch dưới.
        """
        return bool(re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name))

    def _is_valid_column_name(self, col: str) -> bool:
        """
        Kiểm tra dạng column name:
        Cho phép:
          - column
          - table.column
          - database.table.column
        Mỗi phần phải là identifier hợp lệ (_is_valid_identifier).
        """
        parts = col.split('.')
        if len(parts) == 1:
            # chỉ column
            return self._is_valid_identifier(parts[0])
        elif len(parts) == 2:
            # table.column
            table, column = parts
            return self._is_valid_identifier(table) and self._is_valid_identifier(column)
        elif len(parts) == 3:
            # database.table.column
            db, table, column = parts
            return (
                self._is_valid_identifier(db)
                and self._is_valid_identifier(table)
                and self._is_valid_identifier(column)
            )
        else:
            return False
        
parser = SQLParser()
query = "SELECT * FROM db.table.name where name > db.table1.id"

parsed = parser.parse_query(query)
print(parsed["columns"])
print(parsed["tables"])
print(parsed["condition_ast"])