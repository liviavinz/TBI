"""
This script sets up and fills the master tables for the TBI database.
Run this script only once!
"""

import pandas as pd
from tabulate import tabulate

from connection_tbi import TBIConnection
from sqlite_tbi import TBIDatabase

class TBIMasterSetup:

    _ORSTATUS_DATA = [
        (1, 'Geplante Aufnahme'),
        (2, 'Aufnahme'),
        (4, 'Anaesthesie'),
        (6, 'Verfügbar'),
        (7, 'Geschlossen')
    ]

    def __init__(self):
        self.sql_db  = TBIDatabase()
        self.tbi_con = TBIConnection()

    def fill_master_tables(self):
        master_tables = {
            "gender_master": TBIConnection.sql_gender_master,
            "icu_master":    TBIConnection.sql_icu_master,
        }
        with self.sql_db.get_connection() as conn:
            cur = conn.cursor()
            for table_name, (query, pk_col) in master_tables.items():
                df = self.tbi_con.get_data(query)
                if df is not None and not df.empty:
                    cols = list(df.columns)
                    placeholders = ','.join('?' * len(cols))
                    col_list = ','.join(cols)
                    sql = f"INSERT OR REPLACE INTO {table_name} ({col_list}) VALUES ({placeholders})"
                    cur.executemany(sql, df.to_records(index=False).tolist())
                    conn.commit()
                    print(f"  {table_name}: {len(df)} rows upserted")
                else:
                    print(f"  {table_name}: no data returned")

    def fill_orstatus_master(self):
        with self.sql_db.get_connection() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO orstatus_master (ORStatusID, Text) VALUES (?, ?)",
                self._ORSTATUS_DATA,
            )
        print(f"  orstatus_master: {len(self._ORSTATUS_DATA)} rows inserted")

    def show_tables(self):
        with self.sql_db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cur.fetchall()
            print("\nTable overview:")
            for (table_name,) in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                print(f"  {table_name}: {count} rows")

    def show_table_data(self, table_name):
        with self.sql_db.get_connection() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        print(f"\n--- {table_name} ---")
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))

    def run(self):
        self.sql_db.create_tables()
        self.fill_master_tables()
        self.fill_orstatus_master()
        self.show_tables()
        self.show_table_data("gender_master")
        self.show_table_data("icu_master")
        self.show_table_data("orstatus_master")
        print("\nMaster data setup complete — run this script only once!")


if __name__ == "__main__":
    setup = TBIMasterSetup()
    setup.run()
