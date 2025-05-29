import requests
import csv 

class Cursor:
    def __init__(self, url, access_token, refresh_token, db_name) -> None:
        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.session = requests.Session()
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.session.headers.update({
            'Authorization': f'Bearer {self.access_token}',
            'Refresh-Token': self.refresh_token
        })
        self.db_name = db_name

        self.last_result_file = None
        self.index = 0

    def __del__(self):
        self.close()

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
        
    def execute(self, query, path="") -> None:
        response = self.session.post(f'{self.url}/queries/', json={
            'db_name': self.db_name,
            'query': query
        })
        if response.status_code == 200:
            temp = response.json()
            self.index = 0
            self.last_result_file = f"{path}/last_result.csv"
            with open(self.last_result_file, 'w') as file:
                csv_writer = csv.writer(file)
                flag = True
                for item in temp:
                    if flag:
                        csv_writer.writerow(item.keys())
                        flag = False
                    csv_writer.writerow(item.values())
                    
        elif response.status_code == 401:
            self.refresh()
            self.execute(query)
        else:
            raise Exception(f"Query execution failed: {response. status_code} {response.text}")
        
    def fetchone(self) -> dict | None:
        if self.last_result_file is None:
            raise Exception("No query executed yet.")
        
        with open(self.last_result_file, 'r') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)
            for i, row in enumerate(csv_reader):
                if i == self.index:
                    self.index += 1
                    return dict(zip(headers, row))
        return None
        
    def fetchall(self, limit=None) -> list[dict] | None:
        if self.last_result_file is None:
            raise Exception("No query executed yet.")
        
        results = []
        with open(self.last_result_file, 'r') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)
            for i, row in enumerate(csv_reader):
                if limit is not None and i >= limit:
                    break
                results.append(dict(zip(headers, row)))
        return None
        
    def close(self) -> None:
        if self.access_token is None or self.refresh_token is None:
            raise Exception("No active session to close.")
        session_close_response = self.session.get(f'{self.url}/auth/disconnect')
        if session_close_response.status_code != 200:
            raise Exception(f"Failed to close session: {session_close_response.status_code} {session_close_response.text}")
        
        self.session.close()
        self.last_result_file = None
        self.index = 0
        self.access_token = None
        self.refresh_token = None
        self.session.headers.clear()