from typing import List, Dict, Any
import csv


class DB:

    data_file = "/home/phuongnam/Documents/Intern-K-Learning/DB_project/Server-Database-Engine/src/server/database/storage/data.csv"

    def __init__(self) -> None:
        with open(self.data_file, 'r') as file:
            reader = csv.DictReader(file)
            self.data = [row for row in reader]

    def select(self, *args, **kwargs) -> List[Any]:
        """
        - load datafile
        - return all data
        """

        return self.data 


db = DB() 