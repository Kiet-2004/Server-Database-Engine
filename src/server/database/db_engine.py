from server.database.entities.db import DB
from server.config.settings import DB_NAMES

from typing import List, Dict, Any
import csv
import os
import sqlite3




class DatabaseEngine:
    def __init__(self):
        self.db_names = DB_NAMES

    def load_db(self, db_name: str):
        self.db = DB(db_name)


    def query(self, table_name, columns=None, where_clause=None):
        return self.db.tables[table_name].select(columns)


engine = DatabaseEngine()
