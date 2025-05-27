# Represents a node in the expression tree
class ExpressionNode:
    def __init__(self, value, left = None, right = None):
        self.value = value          # The value can be an operator, identifier, or literal
        self.left = left            # Left child (for binary/unary expressions)
        self.right = right          # Right child (None for unary operators)

    def __repr__(self):
        # Display format for debugging: leaf nodes are shown directly;
        # unary operators shown as (OP ARG); binary as (ARG1 OP ARG2)
        if self.left is None and self.right is None:
            return repr(self.value)
        if self.right is None:
            return f"({self.value} {self.left})"
        return f"({self.left} {self.value} {self.right})"

# Builds an AST from a string expression
class AST:
    def __init__(self, expr: str):
        self.tokens = self.tokenize(expr)        # Tokenize the input string
        self.postfix = self.to_postfix(self.tokens)  # Convert to postfix (RPN)
        self.root = self.build_tree(self.postfix)    # Build expression tree from postfix tokens

    def tokenize(self, expr):
        # Lexical analysis: breaks the input string into tokens (numbers, IDs, ops, etc.)
        tokens = []
        i = 0
        while i < len(expr):
            c = expr[i]
            if c.isspace():
                i += 1
                continue
            if c in '()':
                tokens.append((c, c))
                i += 1
            elif c in ['<', '>', '!', '=']:
                # Handles comparison operators like <=, >=, <>, !=
                if c in '<>!' and i + 1 < len(expr) and expr[i + 1] == '=':
                    tokens.append(('OP', expr[i:i + 2]))
                    i += 2
                elif c == '<' and i + 1 < len(expr) and expr[i + 1] == '>':
                    tokens.append(('OP', '<>'))
                    i += 2
                else:
                    tokens.append(('OP', c))
                    i += 1
            elif c in '+-*/%':
                tokens.append(('OP', c))
                i += 1
            elif c == "'":
                # String literal using single quotes
                j = i + 1
                while j < len(expr) and expr[j] != "'":
                    j += 1
                tokens.append(('STRING', expr[i + 1:j]))
                i = j + 1
            elif c == '"':
                # String literal using double quotes
                j = i + 1
                while j < len(expr) and expr[j] != '"':
                    j += 1
                tokens.append(('STRING', expr[i + 1:j]))
                i = j + 1
            elif c.isdigit():
                # Numeric literal (integer or float)
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
                    raise ValueError(f"Invalid numeric literal: {num_str}")
                i = j
            elif c.isalpha() or c == '_' or c == '.' or c == '-':
                # Identifier or keyword
                j = i
                while j < len(expr) and (expr[j].isalnum() or expr[j] in ['_', '.', '-']):
                    j += 1
                word = expr[i:j]
                upper_word = word.upper()
                if upper_word in ['AND', 'OR', 'NOT']:
                    tokens.append((upper_word, upper_word))  # Logical operators
                else:
                    tokens.append(('ID', word))  # Column names or variables
                i = j
            else:
                raise ValueError(f"Invalid character '{c}' in expression")
        return tokens

    def to_postfix(self, tokens):
        # Shunting-yard algorithm to convert infix tokens to postfix
        precedence = {
            'OR': 1,
            'AND': 2,
            'NOT': 3,
            '=': 4, '!=': 4, '<>': 4, '<': 4, '>': 4, '<=': 4, '>=': 4,
            '+': 5, '-': 5,
            '*': 6, '/': 6, '%': 6
        }
        output = []
        stack = []
        for kind, val in tokens:
            if kind in ('ID', 'NUMBER', 'STRING'):
                output.append((kind, val))
            elif kind in ('OP', 'AND', 'OR', 'NOT'):
                while stack and stack[-1][0] != '(' and precedence.get(stack[-1][1], 0) >= precedence[val]:
                    output.append(stack.pop())
                stack.append((kind, val))
            elif kind == '(':
                stack.append((kind, val))
            elif kind == ')':
                while stack and stack[-1][0] != '(':
                    output.append(stack.pop())
                if not stack:
                    raise ValueError("Mismatched parentheses")
                stack.pop()  # Discard the '('
        while stack:
            output.append(stack.pop())
        return output

    def build_tree(self, postfix_tokens):
        # Build the expression tree from postfix tokens
        stack = []
        for kind, value in postfix_tokens:
            if kind in ('ID', 'NUMBER', 'STRING'):
                stack.append(ExpressionNode(value))
            elif value == 'NOT':
                operand = stack.pop()
                stack.append(ExpressionNode(value, operand, None))
            else:
                right = stack.pop()
                left = stack.pop()
                stack.append(ExpressionNode(value, left, right))
        if len(stack) != 1:
            raise ValueError("Invalid expression")
        return stack[0]

# Parses a SQL query string into components: SELECT, FROM, and WHERE
class SQLParser:
    def parse_query(self, query: str):
        # Clean up leading/trailing semicolon and whitespace
        query = query.strip(' ;')

        # Locate positions of SELECT, FROM, WHERE (if any)
        keyword_positions = self._find_keyword_positions(
            query, 
            ["SELECT", "FROM", "WHERE"]
        )
        if 'SELECT' not in keyword_positions or 'FROM' not in keyword_positions:
            raise ValueError("Missing SELECT or FROM clause")

        select_start = keyword_positions['SELECT']
        from_start = keyword_positions['FROM']
        where_start = keyword_positions.get('WHERE')

        # Extract columns
        column_str = query[select_start + len("SELECT"):from_start].strip()
        columns = ["*"] if column_str == "*" else [col.strip() for col in column_str.split(",")]

        # Extract FROM and WHERE clauses
        if where_start:
            from_clause = query[from_start + len("FROM"):where_start].strip()
            where_clause = query[where_start + len("WHERE"):].strip()
        else:
            from_clause = query[from_start + len("FROM"):].strip()
            where_clause = None

        table_names = [name.strip() for name in from_clause.split(",")]

        # Parse WHERE clause into an expression tree
        return {
            "columns": columns,
            "tables": table_names,
            "condition_ast": AST(where_clause).root if where_clause else None
        }

    def _find_keyword_positions(self, query: str, keywords: list[str]) -> dict[str, int]:
        # Find the starting positions of SQL keywords in the original query
        words = query.split()
        positions = {}

        word_index = 0
        kw_index = 0
        sum_pos = 0
        while word_index < len(words) and kw_index < len(keywords):
            if keywords[kw_index].upper() == words[word_index].upper():
                positions[keywords[kw_index]] = sum_pos
                kw_index += 1
            sum_pos += len(words[word_index]) + 1
            word_index += 1
        return positions