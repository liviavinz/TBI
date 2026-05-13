"""
Reads and writes the register (yes/no confirmation) for TBI patients.
"""
import pandas as pd

from schema import REGISTER
from sqlite_tbi import TBIDatabase


class TBIRegister:
    def __init__(self, db: TBIDatabase):
        self.db = db

    def load(self) -> dict[str, dict[str, str]]:
        """
        Return {patient_id_str: {"register_confirmed": "yes"|"no"|""}}.
        Patients not yet in the register table won't appear here.
        """
        with self.db.get_connection() as conn:
            df = pd.read_sql(
                f"SELECT PatientID, register_confirmed FROM {REGISTER.sqlite_table}",
                conn,
            )
        df["register_confirmed"] = df["register_confirmed"].fillna("")
        return {
            str(pid): {"register_confirmed": val}
            for pid, val in zip(df["PatientID"], df["register_confirmed"])
        }

    def save(self, register: dict[str, dict[str, str]]) -> None:
        rows = [
            (int(pid), data.get("register_confirmed") or "")
            for pid, data in register.items()
        ]
        sql = f"""
            INSERT INTO {REGISTER.sqlite_table} (PatientID, register_confirmed)
            VALUES (?, ?)
            ON CONFLICT(PatientID) DO UPDATE SET
                register_confirmed = excluded.register_confirmed
        """
        with self.db.get_connection() as conn:
            conn.executemany(sql, rows)