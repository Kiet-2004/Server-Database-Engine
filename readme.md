# Server Database Engine 🗄️

<h1 align="left">
  <img src="https://st.depositphotos.com/57803962/56729/v/450/depositphotos_567292760-stock-illustration-database-web-icon-simple-illustration.jpg" alt="icon" width="200"></img>
</h1>

## Table of contents
- [Backend](#backend)
  - [Setup environment](#setup-environment)
  - [How to use](#how-to-use)
  - [Demo](#demo)
- [Client](#client)
  - [Setup environment](#setup-environment-1)
  - [How to use](#how-to-use-1)
  - [Demo](#demo-1)

## Backend

The backend for the Server Database Engine is built using **FastAPI**, a modern web framework for building high-performance APIs with Python. **FastAPI** was chosen for its speed, ease of use, and built-in support for asynchronous operations, making it ideal for handling database queries, data storage, and streaming results efficiently.

This backend serves as the core processing unit, managing database operations such as query execution, result streaming, exception handling, and data backup. It communicates with clients via HTTP endpoints, supporting features like authentication and large dataset handling.

### Setup environment

To set up the environment for the Server Database Engine backend, follow these steps:

1. **Install Python**

Ensure you have **Python 3.8 or later** installed on your system. You can check your Python version by running:

```bash
python --version
```

If Python is not installed, download and install it from [python.org](https://www.python.org/downloads/).

2. **Create a Virtual Environment (Recommended)**

It is recommended to create a virtual environment to manage dependencies. Run the following commands:

```bash
python -m venv venv
source venv/bin/activate  # On macOS/Linux
venv\Scripts\activate      # On Windows
```

3. **Install Dependencies**

Once the virtual environment is activated, install the required dependencies. (Note: A `requirements.txt` file may not exist yet; create one with the following content or install manually):

```bash
pip install fastapi uvicorn httpx ijson python-dotenv
```

This will install:

  - **FastAPI** (for building the API)

  - **Uvicorn** (for running the server)

  - **httpx** (for asynchronous HTTP requests)

  - **ijson** (for incremental JSON parsing)

  - **Python-Dotenv** (for managing environment variables)

4. **Set Up Environment Variables**

Create a `.env` file in the project root and configure any necessary variables (e.g., for database paths or API keys if needed):

```python
DATABASE_PATH = /path/to/your/db
```

### How to use

Once the environment is set up, follow these steps to start and use the backend service:

1. **Navigate to the Backend Directory**

Move into the `src/server` directory where the FastAPI application is located (assuming standard structure):

```bash
cd src/server
```

2. **Start the FastAPI Server**

Run the following command to start the server using **Uvicorn**:

```bash
uvicorn app:app --reload
```

The `--reload` flag enables automatic reloading when code changes, making development more efficient. By default, the server runs on `http://127.0.0.1:8000`.

3. **Access the API Documentation**

Once the server is running, you can access the interactive API documentation using **Swagger UI** by navigating to `http://127.0.0.1:8000/docs`. This provides an overview of available endpoints (e.g., `/queries/` for query execution) and allows testing directly from the browser.

To stop the backend server, press `CTRL + C` in the terminal.

### Demo

This section provides a demonstration of how the backend works in action. You can send a POST request to `/queries/` with a JSON payload like `{"db_name": "mydb", "query": "SELECT * FROM table"}` using tools like curl or Postman. The server processes the query and streams JSON results back, handling large datasets efficiently without memory overload.

Example curl command:

```bash
curl -X POST "http://127.0.0.1:8000/queries/" -H "Authorization: Bearer your_token" -d '{"db_name": "mydb", "query": "SELECT * FROM table"}'
```

## Client

The **Client** component of the Server Database Engine is a Python-based interface compliant with **DB-API 2.0** (PEP 249), allowing seamless interaction with the backend server. It handles connections, query execution, result fetching via streaming, and exception management, making it easy to integrate into applications or scripts.

This client is designed for efficiency, using asynchronous operations to manage remote database queries without blocking.

### Setup environment

To set up the environment for the Client, follow these steps:

1. **Install Python**

Ensure you have **Python 3.8 or later** installed (as detailed in the Backend setup).

2. **Create a Virtual Environment (Recommended)**

Use the same virtual environment as the backend or create a new one (see Backend setup).

3. **Install Dependencies**

Install the required dependencies (shared with backend where applicable):

```bash
pip install httpx ijson
```

This will install:

  - **httpx** (for asynchronous HTTP clients)

  - **ijson** (for streaming JSON parsing)

### How to use

Once the setup is complete, follow these steps to use the **Client**:

1. **Navigate to the Client Directory**

Move into the `src/client` directory:

```bash
cd src/client
```

2. **Import and Use the Client in a Script**

Create a Python script (e.g., `example.py`) to connect and query:

```python
import asyncio
from dbapi2 import connect  # Assuming __init__.py exports connect

async def main():
    conn = await connect('http://127.0.0.1:8000', access_token='your_token', refresh_token='your_refresh_token')
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM table")
    results = cursor.fetchall()  # Streams results efficiently
    print(results)
    cursor.close()
    conn.close()

asyncio.run(main())
```

Run the script:

```bash
python example.py
```

***Note***: 

- Ensure the backend server is running before using the client.

- Queries are limited to supported operations; handle exceptions using the built-in handler.

- For large results, the client uses temporary files to avoid memory issues.

### Demo

This section provides a demonstration of how the Client works. After running the backend, execute the sample script above. It will connect to the server, send a query, stream the results using ijson for parsing, and print them. If an error occurs (e.g., unauthorized), it will refresh the token and retry, or raise a custom exception.

Example output (simulated):

```
[{'id': 1, 'name': 'Item1'}, {'id': 2, 'name': 'Item2'}]
```

## Directory Structure

Here is the directory structure of the repository (based on the `client-side` branch, as `main` is minimal):

```
Server-Database-Engine/
├── src/
│   ├── client/
│   │   └── dbapi2/
│   │       ├── __init__.py       # Module initialization and exports
│   │       ├── connection.py     # Connection class for auth and session management
│   │       └── cursor.py         # Cursor class for query execution and streaming fetch
│   └── server/                   # (Inferred; may vary by branch)
│       ├── app.py                # Main FastAPI application
│       ├── database/             # Database logic (storage, backup)
│       └── exceptions/           # Custom exception handlers
├── tests/                        # Unit tests (if present)
├── .gitignore                    # Git ignore file
├── .env                          # Environment variables (not committed)
└── README.md                     # This file
```

# [🏠 Back to top](#server-database-engine-)
