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