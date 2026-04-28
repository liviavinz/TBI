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
            register[pid] = {"register_confirmed": row.get("register_confirmed", "")}
        return register

    def save(self, register: dict):
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                for pid, data in register.items():
                    register_confirmed = data.get("register_confirmed", "") or ""

                    cursor.execute(
                        "SELECT register_id FROM register WHERE PatientID = ?",
                        (int(pid),)
                    )
                    existing = cursor.fetchone()

                    if existing:
                        # always update — allows clearing back to ""
                        cursor.execute("""
                            UPDATE register
                            SET register_confirmed = ?
                            WHERE PatientID = ?
                        """, (register_confirmed, int(pid)))
                    elif register_confirmed:
                        # only insert if there's actually a value
                        cursor.execute("""
                            INSERT INTO register (register_confirmed, PatientID)
                            VALUES (?, ?)
                        """, (register_confirmed, int(pid)))
                conn.commit()
        except Exception as e:
            print(f"ERROR in TBIRegister.save: {e}")
            raise