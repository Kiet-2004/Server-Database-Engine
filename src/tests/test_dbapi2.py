from dbapi2 import connect
import time

start_time = time.time()

conn = connect("http://127.0.0.1:8000", "nam", '12345678', "CRM")


queries = [
    "select * from crm.campaigns"
]

cursor = conn.cursor()


for query in queries:
    try:
        cursor.execute(query)
        print(cursor.fetchmany(5))
    except Exception as e:
        print(f"Error executing query '{query}': {e}")

end_time = time.time()
print(end_time - start_time)