from typing import List, Dict, Any
import csv


class DB:
    def __init__(self, data_file = "./server/database/storage/data.csv") -> None:
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