import httpx
from dbapi2.exceptions import exception_handler, InterfaceError, ProgrammingError
import ijson
import tempfile
import os

class Cursor:
    """A class to execute queries and fetch results from a database connection."""
    def __init__(self, url: str, connection: 'Connect', db_name: str, session: httpx.AsyncClient, cursor_id: int) -> None:
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
        self.array_iterator = None
        self.cursor_id = 1
        self.temp_file_path = None

    def __del__(self) -> None:
        self.close()

    async def execute(self, query: str) -> None:
        """Execute a query and store the streaming JSON results in a temporary file.

        Args:
            query (str): The query to execute.

        Raises:
            InterfaceError: If the session is not initialized.
            DatabaseError: If the query execution fails.
        """
        if self.connection.session is None:
            self.connection.close()
            raise InterfaceError("Session not initialized or closed.")

        # Create a temporary file to store the streaming JSON response
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', encoding='utf-8', delete=False) as temp_file:
            self.temp_file_path = temp_file.name
            try:
                # Stream the POST request
                async with self.session.stream('POST', f'{self.url}/queries/', json={
                    'db_name': self.db_name,
                    'query': query
                }, headers={
                    "Content-Type": "application/json",
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {self.connection.access_token}',
                    'Refresh-Token': self.connection.refresh_token
                }) as response:
                    if response.status_code == 200:
                        # Write streaming response as text to the temporary file
                        async for chunk in response.aiter_text():
                            temp_file.write(chunk)
                        temp_file.flush()
                    elif response.status_code == 401:
                        await self.connection.refresh()
                        self.session.headers = self.connection.headers
                        await self.execute(query)
                        return
                    else:
                        self.connection.close()
                        raise exception_handler(response.json())
            except Exception as e:
                self.connection.close()
                raise e

        # Open the temporary file for reading and set up ijson generator
        self.array_iterator = ijson.items(open(self.temp_file_path, 'r', encoding='utf-8'), 'item')

    def fetchone(self) -> dict | None:
        """Fetch the next result row.

        Returns:
            dict | None: The next row as a dictionary, or None if no more rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if self.array_iterator is None:
            self.connection.close()
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
        if self.array_iterator is None:
            self.connection.close()
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
        if self.array_iterator is None:
            self.connection.close()
            raise ProgrammingError("No query executed yet.")

        results = []
        try:
            while True:
                results.append(next(self.array_iterator))
        except StopIteration:
            return results if results else None

    def close(self) -> None:
        """Close the cursor, its session, and clean up the temporary file."""
        if self.session is not None:
            self.session = None
        if self.array_iterator is not None:
            self.array_iterator = None
        if self.temp_file_path is not None:
            try:
                os.unlink(self.temp_file_path)
            except OSError:
                pass
            self.temp_file_path = None