from server.database.db_engine import engine
from server.utils.exceptions import dpapi2_exception
from server.database.entities.logical_validator import LogicalValidator
from server.database.entities.sql_parser import SQLParser

# Main function to process a user's SQL query.
def query(user_name: str, query: str):

    db_metadata = engine.get_metadata(user_name=user_name)

    parser = SQLParser()
    try:
        # Parse the SQL query string.
        parsed = parser.parse_query(query)
    except dpapi2_exception.ProgrammingError as e:
        # Re-raise programming errors related to SQL syntax.
        raise e

    # Extract parsed components.
    columns, tables, condition_ast = parsed["columns"], parsed["tables"], parsed["condition_ast"]

    # Currently, only single table queries are supported (no joins).
    if len(tables) > 1:
        raise dpapi2_exception.NotSupportedError("Joins between multiple tables are not supported")
    table_name = tables[0] # Get the single table_name name.

    # Dùng LogicalValidator để xác thực và chuẩn hóa logic truy vấn
    validator = LogicalValidator(db_metadata)
    columns, table_name, ast = validator.validate_logic(
        columns = columns,
        table = table_name,
        condition_ast = condition_ast
    )


    return engine.query(
        db_name=list(db_metadata.keys())[0],  # Assuming single database per user.
        columns = columns,
        table_name = table_name,
        ast = condition_ast
    )

    
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