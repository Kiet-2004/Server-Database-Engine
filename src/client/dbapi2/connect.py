from dbapi2.cursor import Cursor
import requests

class Connect:
    def __init__(self, url, access_token, refresh_token, db_name) -> None:
        self.url = url
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.db_name = db_name

    def cursor(self):
        return Cursor(
            url=self.url,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            db_name=self.db_name
        )
    
def connect(url, username, password, db_name):
    full_url = f'{url}/auth/connect'
    params = {'db_name': db_name}
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'scope': '',
        'client_id': '',
        'client_secret': ''
    }
    response = requests.post(full_url, params=params, headers=headers, data=data)
    if response.status_code == 200:
        data = response.json()
        return Connect(
            url=url,
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            db_name=db_name
        )
    else:
        raise Exception(f'Connection failed: {response.status_code} {response.text}')