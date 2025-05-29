from dbapi2 import connect

conn = connect("http://127.0.0.1:8000", "nam", '12345678', "CompanyDB")

cursor = conn.cursor()

cursor.execute("SELECT name FROM employees")
print(cursor.fetchone())
print(cursor.fetchmany(5))
# print(cursor.fetchall())