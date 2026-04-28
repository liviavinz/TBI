"""
This script syncs data from the SQL Server to the TBI SQLite database.
"""
from datetime import datetime, timedelta
import pandas as pd

from connection_tbi import TBIConnection
from sqlite_tbi import TBIDatabase


class TBISync:

    OVERLAP_MIN = 10

    def __init__(self):
        self.tbi_con = TBIConnection()
        self.sql_db  = TBIDatabase()
        self.conn    = None
        self.cur     = None

    # ── Sync state helpers ─────────────────────────────────────────────────
    def _get_last_synced(self, query_name):
        self.cur.execute(
            "SELECT last_synced_at FROM sync_state WHERE query_name = ?",
            (query_name,),
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def _set_last_synced(self, query_name, timestamp):
        self.cur.execute("""
            INSERT INTO sync_state (query_name, last_synced_at) VALUES (?, ?)
            ON CONFLICT(query_name) DO UPDATE SET last_synced_at = excluded.last_synced_at
        """, (query_name, timestamp))

    def _fetch_incremental(self, sql, query_name, patient_ids):
        last_synced = self._get_last_synced(query_name)
        if last_synced:
            cutoff = datetime.fromisoformat(last_synced) - timedelta(minutes=self.OVERLAP_MIN)
            sql = f"{sql} AND TimeStamp > '{cutoff}'"
            print(f"  [{query_name}] incremental since {cutoff}")
        else:
            print(f"  [{query_name}] first run — full load")
        result = self.tbi_con.get_data(sql, patient_ids=patient_ids)
        return result if result is not None else pd.DataFrame()

    # ── Step 1: filter patients ────────────────────────────────────────────
    def _get_filtered_patient_ids(self):
        print("\nSTEP 1: loading consent + IFI + TBI patients...")
        df_consent = self.tbi_con.get_data(TBIConnection.sql_consent)
        if df_consent is None or df_consent.empty:
            print("No consented patients found. Stopping sync.")
            return None
        df_ifi = self.tbi_con.get_data(TBIConnection.sql_ifi)
        if df_ifi is None or df_ifi.empty:
            print("No IFI patients found. Stopping sync.")
            return None
        df_tbi = self.tbi_con.get_data(TBIConnection.sql_tbi)
        if df_tbi is None or df_tbi.empty:
            print("No TBI patients found. Stopping sync.")
            return None
        ids = list(
            set(df_consent['PatientID'].tolist()) &
            set(df_ifi['PatientID'].tolist()) &
            set(df_tbi['PatientID'].tolist())
        )
        print(f"  {len(ids)} patients with consent, IFI and TBI only")
        return ids

    # ── Step 2: connect to SQLite ──────────────────────────────────────────
    def _connect_sqlite(self):
        print("\nSTEP 2: connecting to SQLite...")
        self.conn = self.sql_db.get_connection()
        self.cur  = self.conn.cursor()
        print("  SQLite ready")

    # ── Step 3: insert patients & admissions ──────────────────────────────
    def _insert_patients(self, patient_ids):
        print("\nSTEP 3: inserting patients & admissions...")
        df_patient = self.tbi_con.get_data(TBIConnection.sql_patient)
        df_gender  = self.tbi_con.get_data(TBIConnection.sql_gender)

        df_patient = df_patient[df_patient['PatientID'].isin(patient_ids)]

        df_gender_filtered = (
            df_gender[df_gender['PatientID'].isin(patient_ids)]
            .drop_duplicates(subset='PatientID', keep='last')
        )
        df_patient = df_patient.merge(
            df_gender_filtered[['PatientID', 'TextID']].rename(columns={'TextID': 'gender_TextID'}),
            on='PatientID',
            how='left',
        )

        new_patient_count   = 0
        new_admission_count = 0

        for _, row in df_patient.iterrows():
            patient_id = row['PatientID']
            birthdate  = str(row['BirthDate'])      if pd.notna(row['BirthDate'])      else None
            gender     = int(row['gender_TextID'])  if pd.notna(row['gender_TextID'])  else None
            ssn        = str(row['SocialSecurity']) if pd.notna(row['SocialSecurity']) else None
            adm_date   = str(row['AddmissionDate']) if pd.notna(row['AddmissionDate']) else None
            logical    = int(row['LogicalUnitID'])  if pd.notna(row['LogicalUnitID'])  else None
            or_status  = int(row['ORStatus'])       if pd.notna(row['ORStatus'])       else None
            bed_id     = int(row['BedID'])          if pd.notna(row['BedID'])          else None
            last_name  = str(row['LastName'])       if pd.notna(row['LastName'])       else None
            first_name = str(row['FirstName'])      if pd.notna(row['FirstName'])      else None

            # INSERT into patients (PatientID is PRIMARY KEY → OR IGNORE is enough)
            self.cur.execute("""
                INSERT OR IGNORE INTO patients (PatientID, BirthDate, LastName, FirstName, gender_TextID)
                VALUES (?, ?, ?, ?, ?)
            """, (patient_id, birthdate, last_name, first_name, gender))
            if self.cur.rowcount > 0:
                new_patient_count += 1

            # INSERT into admissions (unique index on PatientID, SSN, ICU, AdmissionDate)
            self.cur.execute("""
                INSERT OR IGNORE INTO admissions
                    (PatientID, SocialSecurity, AdmissionDate, LogicalUnitID, ORStatus, BedID)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (patient_id, ssn, adm_date, logical, or_status, bed_id))
            if self.cur.rowcount > 0:
                new_admission_count += 1

        self.conn.commit()
        print(f"  {len(df_patient)} patient rows processed")
        print(f"  {new_patient_count} new patients inserted")
        print(f"  {new_admission_count} new admissions inserted")

    # ── Steps 4-6: transactional data ─────────────────────────────────────
    def _sync_gcs(self, patient_ids):
        print("\nSTEP 4: fetching transactional data...")
        df_gcs = self._fetch_incremental(TBIConnection.sql_gcs, "gcs", patient_ids)
        print(f"  → gcs rows fetched: {len(df_gcs)}")

        print("\nSTEP 5: inserting transactional data...")
        inserted_gcs = 0
        for _, row in df_gcs.iterrows():
            self.cur.execute("""
                INSERT OR IGNORE INTO gcs_score (PatientID, TimeStamp, Value)
                VALUES (?, ?, ?)
            """, (
                row['PatientID'],
                str(row['TimeStamp']),
                None if pd.isna(row['Value']) else int(float(row['Value'])),
            ))
            inserted_gcs += self.cur.rowcount
        print(f"  → {inserted_gcs} new gcs rows inserted")
        self.conn.commit()

        print("\nSTEP 6: updating sync state...")
        if not df_gcs.empty:
            self._set_last_synced("gcs", str(df_gcs['TimeStamp'].max()))
            print(f"  gcs synced to {df_gcs['TimeStamp'].max()}")
        self.conn.commit()

    # ── Main run ───────────────────────────────────────────────────────────
    def run(self):
        patient_ids = self._get_filtered_patient_ids()
        if patient_ids is None:
            return
        self._connect_sqlite()
        try:
            self._insert_patients(patient_ids)
            self._sync_gcs(patient_ids)
        finally:
            self.conn.close()
        print("\nSync done")


if __name__ == "__main__":
    sync = TBISync()
    sync.run()