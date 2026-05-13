"""
Loads and prepares all data from the TBI SQLite database for the dashboard.

Input : TBIDatabase
Output: TBIData with
  - patients_df       — patient + latest admission + ICU/status lookups
  - gcs_df            — raw time-series of GCS scores
  - overview          — patients_df + latest GCS, latest consents, age, gcs_display
  - patient_options   — list of dicts for Dash dropdown
  - all_patient_ids   — list of PatientIDs in the overview
"""
from datetime import date

import pandas as pd

from schema import (
    PATIENTS, ADMISSIONS, ICU_MASTER, ORSTATUS_MASTER,
    GCS_SCORE, G_CONSENT, IFI_CONSENT,
    G_CONSENT_MASTER, IFI_CONSENT_MASTER,
)
from sqlite_tbi import TBIDatabase


# Columns from each entity that the dashboard wants. Keep these lists short
# and intentional — adding a column to schema.py won't drag it into the UI.
PATIENT_COLS    = ["PatientID", "BirthDate", "LastName", "FirstName"]
ADMISSION_COLS  = ["SocialSecurity", "AdmissionDate", "BedID"]


class TBIData:
    def __init__(self, db: TBIDatabase):
        self.db = db
        self.patients_df     = self._load_patients()
        self.gcs_df          = self._load_gcs()
        self.overview        = self._prepare_overview()
        self.patient_options = self._build_patient_options()
        self.all_patient_ids = self.overview["PatientID"].tolist()
        self.n_patients      = len(self.overview)

    # -- Loaders -------------------------------------------------------------

    def _load_patients(self) -> pd.DataFrame:
        """
        For each patient, attach their most recent admission and resolve
        the ICU and OR-status lookups to human-readable text.
        """
        p_cols = ", ".join(f"p.{c}" for c in PATIENT_COLS)
        a_cols = ", ".join(f"a.{c}" for c in ADMISSION_COLS)
        sql = f"""
            SELECT
                {p_cols},
                {a_cols},
                i.Text AS ICU,
                o.Text AS Status
            FROM {PATIENTS.sqlite_table} p
            LEFT JOIN {ADMISSIONS.sqlite_table} a ON a.admission_id = (
                SELECT admission_id
                FROM {ADMISSIONS.sqlite_table}
                WHERE PatientID = p.PatientID
                ORDER BY AdmissionDate DESC, admission_id DESC
                LIMIT 1
            )
            LEFT JOIN {ICU_MASTER.sqlite_table}      i ON a.LogicalUnitID = i.LogicalUnitID
            LEFT JOIN {ORSTATUS_MASTER.sqlite_table} o ON a.ORStatus      = o.ORStatusID
            ORDER BY p.PatientID
        """
        return self.db.query(sql)

    def _load_gcs(self) -> pd.DataFrame:
        return self.db.query(f"""
            SELECT PatientID, TimeStamp, Value
            FROM {GCS_SCORE.sqlite_table}
            ORDER BY TimeStamp
        """)

    def _load_latest_consent(self, consent_entity, master_entity, out_col: str) -> pd.DataFrame:
        """
        For each patient, return the most recent consent row joined with its
        master text. Returns a DataFrame with columns [PatientID, <out_col>].
        """
        sql = f"""
            SELECT
                c.PatientID,
                m.Text AS {out_col}
            FROM {consent_entity.sqlite_table} c
            JOIN (
                SELECT PatientID, MAX(TimeStamp) AS max_ts
                FROM {consent_entity.sqlite_table}
                GROUP BY PatientID
            ) latest
              ON latest.PatientID = c.PatientID
             AND latest.max_ts   = c.TimeStamp
            LEFT JOIN {master_entity.sqlite_table} m ON c.TextID = m.TextID
        """
        return self.db.query(sql)

    # -- Helpers -------------------------------------------------------------

    @staticmethod
    def _calculate_age(birthdate) -> int | None:
        if pd.isna(birthdate):
            return None
        born = pd.to_datetime(birthdate).date()
        today = date.today()
        return today.year - born.year - (
            (today.month, today.day) < (born.month, born.day)
        )

    @staticmethod
    def _format_gcs_display(value, timestamp) -> str:
        if pd.isna(value):
            return ""
        ts = pd.to_datetime(timestamp).strftime("%d.%m.%Y %H:%M")
        return f"{int(value)} ({ts})"

    # -- Overview ------------------------------------------------------------

    def _prepare_overview(self) -> pd.DataFrame:
        # Latest GCS per patient
        latest_gcs = (
            self.gcs_df.sort_values("TimeStamp")
            .groupby("PatientID").last()
            .reset_index()[["PatientID", "Value", "TimeStamp"]]
            .rename(columns={"Value": "gcs_latest", "TimeStamp": "gcs_latest_date"})
        )

        # Latest consent values per patient (text, not TextID)
        latest_g_consent   = self._load_latest_consent(G_CONSENT,   G_CONSENT_MASTER,   "ConsentGeneral")
        latest_ifi_consent = self._load_latest_consent(IFI_CONSENT, IFI_CONSENT_MASTER, "ConsentIFI")

        df = (
            self.patients_df
            .merge(latest_gcs,          on="PatientID", how="left")
            .merge(latest_g_consent,    on="PatientID", how="left")
            .merge(latest_ifi_consent,  on="PatientID", how="left")
            .reset_index(drop=True)
        )
        df["Age"] = df["BirthDate"].apply(self._calculate_age)
        df["gcs_display"] = df.apply(
            lambda r: self._format_gcs_display(r["gcs_latest"], r["gcs_latest_date"]),
            axis=1,
        )
        df["AdmissionDate_dt"] = pd.to_datetime(df["AdmissionDate"], errors="coerce")

        # ISO string for filtering
        df["AdmissionDate"] = df["AdmissionDate_dt"].dt.strftime("%Y-%m-%d")

        # Swiss format for display
        df["AdmissionDate_display"] = df["AdmissionDate_dt"].dt.strftime("%d.%m.%Y")
        return df

    def _build_patient_options(self) -> list[dict]:
        sorted_overview = self.overview.sort_values(
            ["LastName", "FirstName"], na_position="last"
        )
        options = []
        for _, r in sorted_overview.iterrows():
            last  = r.LastName       if pd.notna(r.LastName)       else "?"
            first = r.FirstName      if pd.notna(r.FirstName)      else "?"
            ssn   = r.SocialSecurity if pd.notna(r.SocialSecurity) else "?"
            options.append({
                "label": f"{last}, {first} (Fallnr. {ssn})",
                "value": r.PatientID,
            })
        return options
