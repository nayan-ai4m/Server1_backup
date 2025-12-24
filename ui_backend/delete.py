import psycopg2

conn = psycopg2.connect(
    host="192.168.1.149",
    database="hul",
    user="postgres",
    password="ai4m2024"
)

cur = conn.cursor()

# Preview count
cur.execute("""
    SELECT COUNT(*) 
    FROM mc29
    WHERE "timestamp" < NOW() - INTERVAL '30 days';
""")
count = cur.fetchone()[0]
print(f"Rows to be deleted: {count}")

# Delete old data
cur.execute("""
    DELETE FROM mc29
    WHERE "timestamp" < NOW() - INTERVAL '30 days';
""")

conn.commit()
cur.close()
conn.close()

print("Old records deleted successfully.")
