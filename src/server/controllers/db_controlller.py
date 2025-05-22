# from utils import query_utils 
from server.database.db_engine import db


def select(query: str):
    # params = query_utils
    # db.select(params)
    return db.select(query)
