# API tests

import asyncio
from dbapi2.connect import connect

async def main():
    conn = connect(
        url="http://127.0.0.1:8000",
        username="string",
        password="stringst",
        db_name="human_resources"
    )
    cursor = conn.cursor()
    await cursor.execute("SELECT * FROM employees where id <= 10;")
    result = cursor.fetchall()
    print(result)

# Run the async code
if __name__ == "__main__":
    asyncio.run(main())