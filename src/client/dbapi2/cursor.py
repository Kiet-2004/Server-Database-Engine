import httpx
from dbapi2.exceptions import exception_handler, InterfaceError, ProgrammingError
from collections.abc import Iterator

class Cursor:
    """A class to execute queries and fetch results from a database connection."""
    def __init__(self, url: str, connection: 'Connect', db_name: str, session: httpx.AsyncClient) -> None:
        """Initialize the cursor with the provided connection and session.

        Args:
            url (str): The base URL of the database API.
            connection (Connect): The parent connection instance.
            db_name (str): The name of the database.
            session (httpx.AsyncClient): The HTTP client session.
        """
        self.url = url
        self.connection = connection
        self.db_name = db_name
        self.session = session
        self.last_result = None
        self.array_iterator = None

    async def execute(self, query: str) -> None:
        """Execute a query and store the results in memory.

        Args:
            query (str): The query to execute.

        Raises:
            InterfaceError: If the session is not initialized.
            DatabaseError: If the query execution fails.
        """
        if self.connection.session is None:
            raise InterfaceError("Session not initialized or closed.")

        
        response = await self.session.post(f'{self.url}/queries/', json={
            'db_name': self.db_name,
            'query': query
        }, headers={
            "Content-Type": "application/json",
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.connection.access_token}',
            'Refresh-Token': self.connection.refresh_token
        })
        if response.status_code == 200:
            self.last_result = response.json()
            self.array_iterator = iter(self.last_result)
        elif response.status_code == 401:
            await self.connection.refresh()
            self.session.headers = self.connection.headers
            await self.execute(query)
        else:
            await self.close()
            raise exception_handler(response.json())


    def fetchone(self) -> dict | None:
        """Fetch the next result row.

        Returns:
            dict | None: The next row as a dictionary, or None if no more rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if self.last_result is None:
            raise ProgrammingError("No query executed yet.")
        try:
            return next(self.array_iterator)
        except StopIteration:
            return None

    def fetchmany(self, size: int = 1) -> list[dict] | None:
        """Fetch the next set of rows of the specified size.

        Args:
            size (int): The number of rows to fetch (default: 1).

        Returns:
            list[dict] | None: A list of row dictionaries, or None if no more rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if self.last_result is None:
            raise ProgrammingError("No query executed yet.")

        results = []
        try:
            for _ in range(size):
                results.append(next(self.array_iterator))
            return results
        except StopIteration:
            return results if results else None

    def fetchall(self) -> list[dict] | None:
        """Fetch all remaining rows.

        Returns:
            list[dict] | None: A list of all remaining row dictionaries, or None if no rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if self.last_result is None:
            raise ProgrammingError("No query executed yet.")

        results = []
        try:
            while True:
                results.append(next(self.array_iterator))
        except StopIteration:
            return results if results else None

    async def close(self) -> None:
        """Close the cursor and its session."""
        if self.session is not None:
            await self.session.aclose()
            self.session = None
            self.last_result = None
            self.array_iterator = None