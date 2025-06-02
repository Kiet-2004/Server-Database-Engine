from server.utils.exceptions import dpapi2_exception
from server.database.entities.ast import AST

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