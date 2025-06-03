import httpx
from dbapi2.cursor import Cursor
from dbapi2.exceptions import InterfaceError, OperationalError

class Connect:
    """A class to manage database connections using an HTTP-based API."""
    def __init__(self, url: str, access_token: str, refresh_token: str, db_name: str) -> None:
        """Initialize the connection with the provided URL, tokens, and database name.

        Args:
            url (str): The base URL of the database API.
            access_token (str): The access token for authentication.
            refresh_token (str): The refresh token for renewing access.
            db_name (str): The name of the database to connect to.
        """
        self.url = url
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.db_name = db_name
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            # 'Authorization': f'Bearer {self.access_token}',
            # 'Refresh-Token': self.refresh_token
        }
        self.session = httpx.AsyncClient(headers=self.headers, timeout=30.0)
        self.cursor_list = []

    async def __aenter__(self):
        """Enter the async context, ensuring the session is initialized."""
        if self.session is None:
            self.session = httpx.AsyncClient(headers=self.headers, timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit the async context, closing the session."""
        await self.close()

    def cursor(self) -> Cursor:
        """Create and return a new cursor for executing queries.

        Returns:
            Cursor: A new cursor instance.

        Raises:
            InterfaceError: If the session is not initialized.
        """
        if self.session is None:
            raise InterfaceError("Session not initialized.")
        cursor = Cursor(
            url=self.url,
            connection=self,
            db_name=self.db_name,
            session=httpx.AsyncClient(timeout=300)
        )
        self.cursor_list.append(cursor)
        return cursor

    async def refresh(self) -> None:
        """Refresh the access and refresh tokens.

        Raises:
            InterfaceError: If the session is not initialized or refresh fails.
        """
        if self.session is None:
            raise InterfaceError("Session not initialized.")
        response = await self.session.post(f'{self.url}/auth/refresh', json={
            'access_token': self.access_token,
            'refresh_token': self.refresh_token
        })
        if response.status_code == 200:
            data = response.json()
            self.access_token = data['access_token']
            self.refresh_token = data['refresh_token']
            self.headers.update({
                'Authorization': f'Bearer {self.access_token}',
                'Refresh-Token': self.refresh_token
            })
            self.session.headers = self.headers
            # Update headers for all cursors
            for cursor in self.cursor_list:
                cursor.session.headers = self.headers
        else:
            raise InterfaceError(f"Failed to refresh tokens: {response.status_code} {response.text}")

    async def close(self) -> None:
        """Close the connection and all associated cursors.

        Raises:
            InterfaceError: If no active session exists.
            OperationalError: If session closure fails.
        """
        if self.access_token is None or self.refresh_token is None:
            raise InterfaceError("No active session to close.")
        if self.session is None:
            raise InterfaceError("Session not initialized.")

        response = await self.session.get(f'{self.url}/auth/disconnect')
        if response.status_code != 200:
            raise OperationalError(f"Failed to close session: {response.status_code} {response.text}")

        for cursor in self.cursor_list:
            await cursor.close()
        self.cursor_list.clear()

        await self.session.aclose()
        self.session = None
        self.access_token = None
        self.refresh_token = None

async def connect(url: str, username: str, password: str, db_name: str) -> Connect:
    """Establish a new database connection.

    Args:
        url (str): The base URL of the database API.
        username (str): The username for authentication.
        password (str): The password for authentication.
        db_name (str): The name of the database to connect to.

    Returns:
        Connect: A new connection instance.

    Raises:
        InterfaceError: If the connection attempt fails.
    """
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

    async with httpx.AsyncClient(timeout=30.0) as session:
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