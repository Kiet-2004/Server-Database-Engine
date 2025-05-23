# from utils import query_utils 
from server.database.db_engine import DATABASE_ENGINE as db


def select(query: str):
    # params = query_utils
    # db.select(params)
    return db.select(query)
