import requests
import csv 

class Cursor:
    def __init__(self, url: str, access_token: str, refresh_token: str, db_name: str, session: requests.Session) -> None:
        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.session = session
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.session.headers.update({
            'Authorization': f'Bearer {self.access_token}',
            'Refresh-Token': self.refresh_token
        })
        self.db_name = db_name
        self.last_result_file = None
        self.array_iterator = None
        self.headers = None

    def refresh(self) -> None:
        response = self.session.post(f'{self.url}/auth/refresh', json={
            'access_token': self.access_token,
            'refresh_token': self.refresh_token
        })
        if response.status_code == 200:
            data = response.json()
            self.access_token = data['access_token']
            self.refresh_token = data['refresh_token']
            self.session.headers.update({
                'Authorization': f'Bearer {self.access_token}',
                'Refresh-Token': self.refresh_token
            })
        else:
            raise Exception(f"Failed to refresh tokens: {response.status_code} {response.text}")
        
    def execute(self, query, path: str = ".") -> None:
        response = self.session.post(f'{self.url}/queries/', json={
            'db_name': self.db_name,
            'query': query
        })
        if response.status_code == 200:
            temp = response.json()
            self.last_result_file = f"{path}/last_result.csv"
            with open(self.last_result_file, 'w') as file:
                csv_writer = csv.writer(file)
                flag = True
                for item in temp:
                    if flag:
                        csv_writer.writerow(item.keys())
                        flag = False
                    csv_writer.writerow(item.values())
            
            self.array_iterator = self._file_generator()
                    
        elif response.status_code == 401:
            self.refresh()
            self.execute(query)
        else:
            raise Exception(f"Query execution failed: {response. status_code} {response.text}")
        
    def _file_generator(self):
        if self.last_result_file is None:
            raise Exception("No query executed yet.")

        with open(self.last_result_file, 'r') as file:
            csv_reader = csv.reader(file)
            self.headers = next(csv_reader)
            for row in csv_reader:
                yield dict(zip(self.headers, row))
        
    def fetchone(self) -> dict | None:
        if self.last_result_file is None:
            raise Exception("No query executed yet.")
        try:
            result = next(self.array_iterator)
            return result
        except StopIteration:
            return None
    
    def fetchmany(self, size: int = 1) -> list[dict] | None:
        if self.last_result_file is None:
            raise Exception("No query executed yet.")
        
        results = []
        try:
            for _ in range(size):
                result = next(self.array_iterator)
                results.append(result)
            return results
        except StopIteration:
            return results if results else None

    def fetchall(self) -> list[dict] | None:
        if self.last_result_file is None:
            raise Exception("No query executed yet.")
        
        results = []
        try:
            while True:
                result = next(self.array_iterator)
                results.append(result)
        except StopIteration:
            return results if results else None
    