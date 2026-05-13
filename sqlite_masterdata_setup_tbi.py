"""
Sets up and fills the master (lookup) tables for the TBI database.

Run this script ONCE after creating the SQLite database.

It loops over schema.master_entities() and dispatches on entity.kind:
  - master_from_sql -> fetched from SQL Server via TBIConnection
  - master_static   -> hardcoded rows in schema.py
"""
import pandas as pd
from tabulate import tabulate

from connection_tbi import TBIConnection
from schema import Entity, master_entities
from sqlite_tbi import TBIDatabase


class TBIMasterSetup:
    def __init__(self):
        self.sql_db = TBIDatabase()
        self.tbi_con = TBIConnection()

    # -- Filling -------------------------------------------------------------

    def _rows_for(self, entity: Entity) -> list[tuple] | None:
        """Return the rows to insert for a master entity, or None on failure."""
        if entity.kind == "master_static":
            return entity.static_data or []

        if entity.kind == "master_from_sql":
            df = self.tbi_con.fetch(entity)
            if df is None:
                return None
            # Keep only the columns this entity declares, in declared order
            cols = list(entity.columns.keys())
            df = df[cols]
            return df.to_records(index=False).tolist()

        raise ValueError(f"_rows_for called on non-master entity: {entity.name}")

    def fill_master_tables(self):
        with self.sql_db.get_connection() as conn:
            cur = conn.cursor()
            for entity in master_entities():
                rows = self._rows_for(entity)
                if rows is None:
                    print(f"  {entity.name}: query failed")
                    continue
                if not rows:
                    print(f"  {entity.name}: no data")
                    continue

                cols = list(entity.columns.keys())
                col_list = ",".join(cols)
                placeholders = ",".join("?" * len(cols))
                sql = (
                    f"INSERT OR REPLACE INTO {entity.sqlite_table} "
                    f"({col_list}) VALUES ({placeholders})"
                )
                cur.executemany(sql, rows)
                conn.commit()
                print(f"  {entity.name}: {len(rows)} rows upserted")

    # -- Inspection ----------------------------------------------------------

    def show_tables(self):
        with self.sql_db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            print("\nTable overview:")
            for (table_name,) in cur.fetchall():
                if table_name.startswith("sqlite_"):
                    continue
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                print(f"  {table_name}: {cur.fetchone()[0]} rows")

    def show_table_data(self, table_name: str):
        with self.sql_db.get_connection() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        print(f"\n--- {table_name} ---")
        print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))

    # -- Run -----------------------------------------------------------------

    def run(self):
        self.sql_db.create_tables()
        self.fill_master_tables()
        self.show_tables()
        for entity in master_entities():
            self.show_table_data(entity.sqlite_table)
        print("\nMaster data setup complete — run this script only once!")


if __name__ == "__main__":
    TBIMasterSetup().run()