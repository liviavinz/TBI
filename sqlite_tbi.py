"""
Creates the local SQLite database and its tables.

Schema is NOT defined here — it lives in schema.py. This file just
walks the schema and emits the corresponding CREATE TABLE / CREATE INDEX
statements. Add a column or a table by editing schema.py only.
"""
import os
import sqlite3
from pathlib import Path

import pandas as pd

from schema import Entity, entities_with_sqlite_table


class TBIDatabase:
    DB_PATH = os.getenv(
        "SQLITE_DB_PATH",
        str(Path(__file__).parent / "data" / "tbi_database.sqlite"),
    )

    # -- Connection ----------------------------------------------------------

    def get_connection(self):
        Path(self.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    # -- DDL generation ------------------------------------------------------

    @staticmethod
    def _create_table_sql(entity: Entity) -> str:
        """Build a CREATE TABLE IF NOT EXISTS statement for one entity."""
        parts = [f"{name} {typ}" for name, typ in entity.columns.items()]
        parts.extend(entity.foreign_keys)
        body = ",\n  ".join(parts)
        return f"CREATE TABLE IF NOT EXISTS {entity.sqlite_table} (\n  {body}\n);"

    @staticmethod
    def _create_index_sql(entity: Entity) -> list[str]:
        """Build CREATE UNIQUE INDEX statements for one entity."""
        stmts = []
        for cols in entity.unique_indexes:
            idx_name = f"ux_{entity.sqlite_table}_" + "_".join(cols)
            col_list = ", ".join(cols)
            stmts.append(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} "
                f"ON {entity.sqlite_table} ({col_list});"
            )
        return stmts

    def generate_ddl(self) -> str:
        """Return the full DDL script as a single string. Useful for debugging."""
        parts = []
        for entity in entities_with_sqlite_table():
            parts.append(self._create_table_sql(entity))
            parts.extend(self._create_index_sql(entity))
        return "\n\n".join(parts)

    # -- Execution -----------------------------------------------------------

    def create_tables(self):
        """Create every SQLite table and index defined in schema.py."""
        ddl = self.generate_ddl()
        with self.get_connection() as conn:
            conn.executescript(ddl)

    def query(self, sql: str) -> pd.DataFrame:
        with self.get_connection() as conn:
            return pd.read_sql(sql, conn)


if __name__ == "__main__":
    db = TBIDatabase()
    print(db.generate_ddl())
    print("\n--- creating tables ---")
    db.create_tables()
    print(f"Done. Database at: {db.DB_PATH}")