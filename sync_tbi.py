"""
Syncs data from SQL Server to the local SQLite database.

The logic is schema-driven: for each transactional entity in schema.py,
this script picks the right strategy (full reload vs incremental) and
the right conflict behavior (ignore vs update) based on entity fields.

To add a new synced table, edit schema.py only.
"""
from datetime import datetime, timedelta
import decimal
import pandas as pd

from connection_tbi import TBIConnection
from schema import Entity, transactional_entities
from sqlite_tbi import TBIDatabase


class TBISync:
    # When doing incremental sync, re-fetch a small overlap window
    # to catch late-arriving rows from the source system.
    OVERLAP_MIN = 10

    def __init__(self):
        self.tbi_con = TBIConnection()
        self.sql_db = TBIDatabase()
        self.conn = None
        self.cur = None

    # -- sync_state helpers --------------------------------------------------

    def _get_last_synced(self, entity_name: str) -> str | None:
        self.cur.execute(
            "SELECT last_synced_at FROM sync_state WHERE query_name = ?",
            (entity_name,),
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def _set_last_synced(self, entity_name: str, timestamp: str) -> None:
        self.cur.execute(
            """
            INSERT INTO sync_state (query_name, last_synced_at) VALUES (?, ?)
            ON CONFLICT(query_name) DO UPDATE SET last_synced_at = excluded.last_synced_at
            """,
            (entity_name, timestamp),
        )

    # -- Fetching ------------------------------------------------------------

    def _fetch(self, entity: Entity, patient_ids: set[int]) -> pd.DataFrame:
        """Fetch one entity, applying incremental cutoff if configured."""
        if entity.incremental_column is None:
            print(f"  [{entity.name}] full reload")
            df = self.tbi_con.fetch(entity, patient_ids=patient_ids)
            return df if df is not None else pd.DataFrame()

        # Incremental: ask only for rows newer than last sync (with overlap)
        last = self._get_last_synced(entity.name)
        sql = entity.source_query
        if last:
            cutoff = datetime.fromisoformat(last) - timedelta(minutes=self.OVERLAP_MIN)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            sql = (
                f"SELECT * FROM ({sql}) AS sub "
                f"WHERE {entity.incremental_column} > '{cutoff_str}'"
            )
            print(f"  [{entity.name}] incremental since {cutoff}")
        else:
            print(f"  [{entity.name}] first run — full load")

        df = self.tbi_con.get_data(sql, patient_ids=patient_ids)
        if df is None:
            return pd.DataFrame()
        # Apply column rename so the DataFrame matches the SQLite schema
        if entity.column_map:
            df = df.rename(columns=entity.column_map)
        return df

    # -- Inserting -----------------------------------------------------------

    @staticmethod
    def _insert_columns(entity: Entity) -> list[str]:
        """Columns to INSERT into — skip AUTOINCREMENT id columns."""
        return [
            name for name, typ in entity.columns.items()
            if "AUTOINCREMENT" not in typ.upper()
        ]

    @staticmethod
    def _build_insert_sql(entity: Entity) -> str:
        """Build the right INSERT statement for the entity's on_conflict mode."""
        cols = TBISync._insert_columns(entity)
        col_list = ",".join(cols)
        placeholders = ",".join("?" * len(cols))

        if entity.on_conflict == "ignore":
            return (
                f"INSERT OR IGNORE INTO {entity.sqlite_table} "
                f"({col_list}) VALUES ({placeholders})"
            )

        if entity.on_conflict == "update":
            # Conflict target = the columns of the entity's first unique index.
            # That's the natural key (e.g. (PatientID, AdmissionDate)).
            if not entity.unique_indexes:
                raise ValueError(
                    f"{entity.name}: on_conflict='update' requires a unique_indexes entry"
                )
            conflict_cols = ",".join(entity.unique_indexes[0])
            update_cols = [c for c in cols if c not in entity.unique_indexes[0]]
            update_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
            return (
                f"INSERT INTO {entity.sqlite_table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict_cols}) DO UPDATE SET {update_clause}"
            )

        raise ValueError(f"{entity.name}: unknown on_conflict mode {entity.on_conflict!r}")

    def _upsert(self, entity: Entity, df: pd.DataFrame) -> int:
        """Insert/update a DataFrame's rows into the entity's SQLite table."""
        if df.empty:
            return 0
        cols = self._insert_columns(entity)
        # Keep only the columns we insert, in declared order; missing ones → NaN
        df = df.reindex(columns=cols)
        # Convert NaN → None so SQLite stores NULL instead of the string "nan"
        import decimal

        def _coerce(v):
            if pd.isna(v):
                return None
            if isinstance(v, pd.Timestamp):
                return v.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(v, decimal.Decimal):
                return int(v) if v == v.to_integral_value() else float(v)
            return v

        rows = [tuple(_coerce(v) for v in row)
                for row in df.itertuples(index=False, name=None)]

        sql = self._build_insert_sql(entity)
        self.cur.executemany(sql, rows)
        return self.cur.rowcount  # for INSERT OR IGNORE: count of new rows

    # -- Per-entity sync -----------------------------------------------------

    def _sync_entity(self, entity: Entity, cohort: set[int]) -> None:
        df = self._fetch(entity, patient_ids=cohort)
        n = self._upsert(entity, df)
        self.conn.commit()
        print(f"  [{entity.name}] {len(df)} fetched, {n} inserted/updated")

        # Advance the sync cursor for incremental entities
        if entity.incremental_column is not None:
            if not df.empty and entity.incremental_column in df.columns:
                new_cursor = str(df[entity.incremental_column].max())
            else:
                # No new rows — advance to now() so we don't re-scan the same window
                new_cursor = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._set_last_synced(entity.name, new_cursor)
            self.conn.commit()
            print(f"  [{entity.name}] sync cursor → {new_cursor}")

    # -- Main run ------------------------------------------------------------

    def run(self) -> None:
        print("\nSTEP 1: computing cohort...")
        cohort = self.tbi_con.get_cohort()
        if not cohort:
            print("Empty cohort — nothing to sync.")
            return

        print("\nSTEP 2: connecting to SQLite...")
        self.conn = self.sql_db.get_connection()
        self.cur = self.conn.cursor()

        try:
            print("\nSTEP 3: syncing transactional entities...")
            for entity in transactional_entities():
                self._sync_entity(entity, cohort)
        finally:
            self.conn.close()

        print("\nSync done.")


if __name__ == "__main__":
    TBISync().run()

