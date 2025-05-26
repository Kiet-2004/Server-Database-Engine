from typing import List, Dict, Any
import csv
import os
from server.config import settings


class DB:
    def __init__(self, data_file = "data.csv") -> None:
        data_file = os.path.join(settings.STORAGE_FOLDER, data_file)
        with open(data_file, 'r') as file:
            reader = csv.DictReader(file)
            self.data = [row for row in reader]

    def select(self, *args, **kwargs) -> List[Any]:
        """
        - load datafile
        - return all data
        """

        return self.data 


DATABASE_ENGINE = DB() 