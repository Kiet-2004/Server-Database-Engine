# API tests

import asyncio
import time
from dbapi2.connect import connect

async def main():
    start_time_1 = time.time()
    async with await connect(
        url="http://127.0.0.1:8000",
        username="string",
        password="stringst",
        db_name="CRM"
    ) as conn:
        start_time_2 = time.time()
        print(f"Connection time: {start_time_2 - start_time_1} (s)")
        cursor = conn.cursor()
        start_time_3 = time.time()
        print(f"Creating cursor time: {start_time_3 - start_time_2} (s)")
        await cursor.execute("SELECT * from CRM.campaigns where CRM.campaigns.CampaignID > 1000")
        start_time_4 = time.time()
        print(f"Executing time: {start_time_4 - start_time_3} (s)")
        result = cursor.fetchone()
        print(result)
        start_time_5 = time.time()
        print(f"Fetching time: {start_time_5 - start_time_4} (s)")
    
    end_time = time.time()
    print(f"Total time: {end_time - start_time_1} (s)")

# Run the async code
if __name__ == "__main__":
    asyncio.run(main())