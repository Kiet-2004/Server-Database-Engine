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

        self.last_result = None
        self.index = 0

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
        
    def execute(self, query, path) -> None:
        response = self.session.post(f'{self.url}/queries/', json={
            'db_name': self.db_name,
            'query': query
        })
        if response.status_code == 200:
            self.last_result = response.json()
            self.index = 0
            with open(f"{path}/last_result.csv", 'w') as file:
                csv_writer = csv.writer(file)
                flag = True
                for item in self.last_result:
                    if flag:
                        csv_writer.writerow(item.keys())
                        flag = False
                    csv_writer.writerow(item.values())
                    
        elif response.status_code == 401:
            self.refresh()
            self.execute(query)
        else:
            raise Exception(f"Query execution failed: {response. status_code} {response.text}")
        
    def fetchone(self):
        if self.last_result is None:
            raise Exception("No query executed yet.")
        
        if self.index < len(self.last_result):
            result = self.last_result[self.index]
            self.index += 1
            return result
        else:
            return None
        
    def fetchall(self, limit=None):
        if self.last_result is None:
            raise Exception("No query executed yet.")
        
        if limit is None:
            return self.last_result
        else:
            return self.last_result[:limit]
        
    def close(self):
        self.session.close()
        self.last_result = None
        self.index = 0
        self.access_token = None
        self.refresh_token = None
        self.session.headers.clear()