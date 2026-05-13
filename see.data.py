import sqlite3
from sqlite_tbi import TBIDatabase

db = TBIDatabase()
conn = sqlite3.connect(db.DB_PATH)

# List all tables
tables = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()
print("Tables:")
for (t,) in tables:
    print(f"  {t}")

# List all indexes (skip SQLite internal ones)
indexes = conn.execute(
    "SELECT name, tbl_name FROM sqlite_master "
    "WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
).fetchall()
print("\nIndexes:")
for name, tbl in indexes:
    print(f"  {name}  (on {tbl})")

# See the actual columns of one table
print("\nColumns of admissions:")
for row in conn.execute("PRAGMA table_info(admissions)").fetchall():
    cid, name, typ, notnull, default, pk = row
    print(f"  {name:20s} {typ}")

conn.close()