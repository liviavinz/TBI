import sqlite3
import pandas as pd

DB_PATH = '/home/vinzl/Documents/tbi_database.sqlite'
conn = sqlite3.connect(DB_PATH)
print(pd.read_sql("SELECT * FROM register", conn).to_string())
conn.close()