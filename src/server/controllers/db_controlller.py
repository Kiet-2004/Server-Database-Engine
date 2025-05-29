from server.database.db_engine import engine
from server.utils.exceptions import dpapi2_exception
from server.utils.query_utils import SQLParser

# Main function to process a user's SQL query.
def query(user_name: str, query: str):
    parser = SQLParser()
    try:
        # Parse the SQL query string.
        parsed = parser.parse_query(query)
    except dpapi2_exception.ProgrammingError as e:
        # Re-raise programming errors related to SQL syntax.
        raise e

    # Extract parsed components.
    columns = parsed["columns"]
    tables = parsed["tables"]
    condition_ast = parsed["condition_ast"]

    # Currently, only single table queries are supported (no joins).
    if len(tables) > 1:
        raise dpapi2_exception.NotSupportedError("Joins between multiple tables are not supported")

    table = tables[0] # Get the single table name.

    try:
        # Call the database engine's query method with the parsed components.
        return engine.query(
            user_name = user_name,
            columns = columns,
            table_name = table,
            ast = condition_ast
        )
    except (
        dpapi2_exception.InterfaceError,
        dpapi2_exception.DatabaseError,
        dpapi2_exception.DataError,
        dpapi2_exception.OperationalError,
        dpapi2_exception.IntegrityError,
        dpapi2_exception.InternalError,
        dpapi2_exception.ProgrammingError,
        dpapi2_exception.NotSupportedError,
    ) as e:
        # Re-raise any database-related exceptions.
        raise e
    
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