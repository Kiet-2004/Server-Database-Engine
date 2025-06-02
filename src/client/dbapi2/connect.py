import httpx
from dbapi2.cursor import Cursor
from dbapi2.exceptions import InterfaceError, OperationalError

class Connect:
    def __init__(self, url: str, access_token: str, refresh_token: str, db_name: str) -> None:
        self.url = url
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.db_name = db_name
        self.session = None

    async def __aenter__(self):
        self.session = httpx.AsyncClient(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def cursor(self) -> Cursor:
        if self.session is None:
            raise InterfaceError("Session not initialized. Use 'async with' to initialize the connection.")
        return Cursor(
            url=self.url,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            db_name=self.db_name,
            session=self.session
        )
    
    async def close(self) -> None:
        if self.access_token is None or self.refresh_token is None:
            raise InterfaceError("No active session to close.")
        if self.session is None:
            raise InterfaceError("Session not initialized.")
        
        response = await self.session.get(f'{self.url}/auth/disconnect')
        if response.status_code != 200:
            raise OperationalError(f"Failed to close session: {response.status_code} {response.text}")
        
        await self.session.aclose()
        self.access_token = None
        self.refresh_token = None

async def connect(url: str, username: str, password: str, db_name: str) -> Connect:
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
    
    async with httpx.AsyncClient() as session:
        response = await session.post(full_url, params=params, headers=headers, data=data)
        if response.status_code == 200:
            data = response.json()
            return Connect(
                url=url,
                access_token=data['access_token'],
                refresh_token=data['refresh_token'],
                db_name=db_name
            )
        else:
            raise InterfaceError(f'Connection failed: {response.status_code} {response.text}')