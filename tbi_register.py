"""
Handles reading and writing the register (yes/no) for TBI patients.
"""
import pandas as pd
from sqlite_tbi import TBIDatabase


class TBIRegister:

    def __init__(self, db: TBIDatabase):
        self.db = db

    def load(self) -> dict:
        with self.db.get_connection() as conn:
            df = pd.read_sql("SELECT * FROM register", conn)
        register = {}
        for _, row in df.iterrows():
            pid = str(row["PatientID"])
            val = row["register_confirmed"]
            register[pid] = {
                "register_confirmed": val if pd.notna(val) else ""
            }
        return register

    def save(self, register: dict):
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                for pid, data in register.items():
                    register_confirmed = data.get("register_confirmed", "") or ""

                    cursor.execute("""
                        INSERT INTO register (PatientID, register_confirmed)
                        VALUES (?, ?)
                        ON CONFLICT(PatientID) DO UPDATE SET
                            register_confirmed = excluded.register_confirmed
                    """, (int(pid), register_confirmed))
                conn.commit()
        except Exception as e:
            print(f"ERROR in TBIRegister.save: {e}")
            raise