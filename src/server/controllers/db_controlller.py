# from utils import query_utils 
from server.database.db_engine import engine
from server.utils.exceptions import dpapi2_exception



import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML

def extract_tokens(parsed):
    """
    Extract SELECT columns, FROM table, and WHERE clause from parsed query.
    """
    columns, table, where = [], None, None

    for token in parsed.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            continue
        if token.ttype is Keyword and token.value.upper() == "FROM":
            continue

        if isinstance(token, IdentifierList):
            columns = [str(i).strip() for i in token.get_identifiers()]
        elif isinstance(token, Identifier) and not table:
            table = str(token)
        elif isinstance(token, Where):
            where = str(token)[6:]  # remove 'WHERE '

    return columns, table.strip(), where.strip() if where else None

# def connect(db_name: str):
def disconnect_user(user_name: str):
    """
    Disconnect a user from the database by removing their connection.
    """
    try:
        return engine.disconnect_user(user_name)
    except dpapi2_exception.DatabaseError as e:
        raise dpapi2_exception.DatabaseError(str(e))

def connect_user(user_name: str, db_name: str):
    """
    Connect a user to a database.
    """
    try:
        engine.load_db(user_name=user_name, db_name=db_name)
        return {"message": f"User {user_name} connected to database {db_name} successfully."}
    except dpapi2_exception.DatabaseError as e:
        raise dpapi2_exception.DatabaseError(str(e))

def query(user_name: str, query: str):
    parsed = sqlparse.parse(query)[0]
    columns, table, where_clause = extract_tokens(parsed)

    return engine.query(
        user_name=user_name,
        table_name=table,
        columns=columns if columns else None,
        ast=where_clause
    )
