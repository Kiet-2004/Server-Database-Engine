from dbapi2 import connect

conn = connect("http://127.0.0.1:8000", "nam", '12345678', "CompanyDB")


queries = [
    "SELECT * FROM employees WHERE name = 'Alice';",
    "SELECT id FROM employees WHERE phone LIKE '090%';",
    "SELECT email FROM employees WHERE email LIKE '%@company.com';",
    "SELECT name FROM employees WHERE id = 5;",
    "SELECT phone FROM employees WHERE name = 'Bob';",
    "SELECT * FROM employees WHERE email IS NOT NULL;",
    "SELECT name FROM employees WHERE phone IS NULL;",
    "SELECT * FROM employees WHERE name LIKE 'J%';",
    "SELECT * FROM employees WHERE email LIKE '%.vn';",
    "SELECT id FROM employees WHERE name != 'John';",
    "SELECT * FROM task WHERE status = 'completed';",
    "SELECT task FROM task WHERE hours > 5.0;",
    "SELECT id FROM task WHERE task LIKE '%report%';",
    "SELECT * FROM task WHERE status = 'pending';",
    "SELECT hours FROM task WHERE task = 'Design UI';",
    "SELECT * FROM task WHERE hours < 2.5;",
    "SELECT task FROM task WHERE status != 'completed';",
    "SELECT * FROM task WHERE task LIKE 'Fix%';",
    "SELECT id FROM task WHERE hours = 8.0;",
    "SELECT * FROM task WHERE status IS NOT NULL;"
]

cursor = conn.cursor()


for query in queries:
    try:
        cursor.execute(query)
        print(cursor.fetchmany(5))
    except Exception as e:
        print(f"Error executing query '{query}': {e}")

# print(cursor.fetchall())