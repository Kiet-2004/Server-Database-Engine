# from utils import query_utils 
from server.database.db_engine import engine


def select(query: str):
    # params = query_utils
    # db.select(params)
    return engine.query(table_name='employees', columns=None)
