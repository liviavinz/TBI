"""
Loads and prepares all data from the TBI SQLite database for the dashboard.
Input: TBIDatabase object
Output: TBIData with
- patients data frame
- gcs data frame
- overview data frame (patients + gcs latest)
- patient options; list of dicts for Dash dropdown
- all patient ids
"""
import pandas as pd
from sqlite_tbi import TBIDatabase
from datetime import date

class TBIData:

    def __init__(self, db: TBIDatabase):
        self.db              = db
        self.patients_df     = self._load_patients()
        self.gcs_df          = self._load_gcs()
        self.overview        = self._prepare_overview()
        self.patient_options = self._build_patient_options()
        self.all_patient_ids = self.overview["PatientID"].tolist()
        self.n_patients      = len(self.overview)

    # ── Loaders ────────────────────────────────────────────────────────────
    def _load_patients(self) -> pd.DataFrame:
        return self.db.query("""
            SELECT
                p.PatientID,
                p.BirthDate,
                p.LastName,
                p.FirstName,
                a.SocialSecurity,
                a.AdmissionDate,
                a.BedID,
                i.Text AS ICU,
                o.Text AS Status,
                g.Text AS Gender
            FROM patients p
            LEFT JOIN admissions a ON a.admission_id = (
                SELECT admission_id
                FROM admissions
                WHERE PatientID = p.PatientID
                ORDER BY AdmissionDate DESC, admission_id DESC
                LIMIT 1
            )
            LEFT JOIN icu_master      i ON a.LogicalUnitID = i.LogicalUnitID
            LEFT JOIN orstatus_master o ON a.ORStatus      = o.ORStatusID
            LEFT JOIN gender_master   g ON p.gender_TextID = g.TextID
            ORDER BY p.PatientID
        """)

    def _load_gcs(self) -> pd.DataFrame:
        return self.db.query("""
            SELECT PatientID, TimeStamp, Value
            FROM gcs_score
            ORDER BY TimeStamp
        """)

    # ── Overview ───────────────────────────────────────────────────────────
    @staticmethod
    def _calculate_age(birthdate):
        if pd.isna(birthdate):
            return None
        born = pd.to_datetime(birthdate).date()
        today = date.today()
        return today.year - born.year - (
                (today.month, today.day) < (born.month, born.day)
        )

    @staticmethod
    def _format_dt(value, fmt="%d.%m.%Y %H:%M"):
        """Format a datetime value, return empty string for missing values."""
        if pd.isna(value):
            return ""
        try:
            return pd.to_datetime(value).strftime(fmt)
        except (ValueError, TypeError):
            return ""

    # ── Overview ───────────────────────────────────────────────────────────
    def _prepare_overview(self) -> pd.DataFrame:
        latest_gcs = (
            self.gcs_df.sort_values("TimeStamp")
            .groupby("PatientID").last()
            .reset_index()[["PatientID", "Value", "TimeStamp"]]
            .rename(columns={"Value": "gcs_latest", "TimeStamp": "gcs_latest_date"})
        )
        df = (
            self.patients_df
            .merge(latest_gcs, on="PatientID", how="left")
            .reset_index(drop=True)
        )

        # calculate age from BirthDate
        df["Age"] = df["BirthDate"].apply(self._calculate_age)

        # combine value and date into one display column
        df["gcs_display"] = df.apply(
            lambda r: (
                f"{int(r['gcs_latest'])} "
                f"({pd.to_datetime(r['gcs_latest_date']).strftime('%d.%m.%Y %H:%M')})"
            )
            if pd.notna(r["gcs_latest"]) else "",
            axis=1,
        )
        return df

    def _build_patient_options(self) -> list:
        sorted_overview = self.overview.sort_values(["LastName", "FirstName"], na_position="last")
        options = []
        for _, r in sorted_overview.iterrows():
            last = r.LastName if pd.notna(r.LastName) else "?"
            first = r.FirstName if pd.notna(r.FirstName) else "?"
            ssn = r.SocialSecurity if pd.notna(r.SocialSecurity) else "?"
            options.append({
                "label": f"{last}, {first} (Fallnr. {ssn})",
                "value": r.PatientID,
            })
        return options


