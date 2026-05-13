"""
Single source of truth for all TBI entities.

Every stage of the pipeline reads from this module:
  1. connection_tbi.py        -> uses Entity.source_query to fetch from SQL Server
  2. sqlite_tbi.py            -> generates CREATE TABLE / indexes from Entity.columns
  3. sqlite_masterdata_setup  -> loads master_from_sql + master_static entities
  4. sync_tbi.py              -> UPSERTs transactional entities into SQLite
  5. tbi_data.py / dashboard  -> reads column names from the entities

To add a new attribute or table: edit ONLY this file.
"""

from dataclasses import dataclass, field
from typing import Literal

EntityKind = Literal[
    "filter",            # returns PatientIDs only; no SQLite table created
    "master_from_sql",   # lookup table whose rows come from SQL Server
    "master_static",     # lookup table whose rows are hardcoded here
    "transactional",     # data table populated by sync from SQL Server
    "local_only",        # SQLite-only table (no source query, e.g. register, sync_state)
]

@dataclass
class Entity:
    name: str
    kind: EntityKind
    sqlite_table: str | None = None         # None for "filter" entities
    columns: dict[str, str] = field(default_factory=dict)
    source_query: str | None = None         # SQL Server query
    static_data: list[tuple] | None = None  # rows for master_static
    primary_key: list[str] = field(default_factory=list)
    column_map: dict[str, str] = field(default_factory=dict)
    # ^ rename source column -> SQLite column (e.g. AddmissionDate -> AdmissionDate)
    foreign_keys: list[str] = field(default_factory=list)
    unique_indexes: list[list[str]] = field(default_factory=list)
    # ^ extra unique indexes beyond the PK (each is a list of column names)
    incremental_column: str | None = None
    # ^ for transactional entities: which column to use as the sync cursor.
    #   None means full reload every sync (no time column or table is small).
    on_conflict: str = "ignore"
    # ^ "ignore" = INSERT OR IGNORE (skip duplicates)
    #   "update" = INSERT ... ON CONFLICT(...) DO UPDATE (overwrite duplicates)

# ---------------------------------------------------------------------------
# Cohort filters
# ---------------------------------------------------------------------------
# Each filter returns a set of PatientIDs. The cohort is the intersection
# of all filters. Every transactional query is then restricted to that set,
# so no per-entity date logic is needed.
#
# Change the cutoff date once in TIME_FILTER below.
# ---------------------------------------------------------------------------
MIN_DATE = "2026-05-01"

TIME_FILTER = Entity(
    name="time_filter",
    kind="filter",
    source_query=f"""
        SELECT DISTINCT PatientID
        FROM dbo.DimPatient
        WHERE AddmissionDate >= '{MIN_DATE}'
    """,
)

IFI_FILTER = Entity(
    name="ifi_filter",
    kind="filter",
    source_query="""
        SELECT DISTINCT PatientID
        FROM dbo.DimPatient
        WHERE LogicalUnitID IN (1, 46, 48, 49, 50, 51, 68, 69)
    """,
)

TBI_FILTER = Entity(
    name="tbi_filter",
    kind="filter",
    source_query="""
        SELECT DISTINCT PatientID
        FROM dbo.FactSignal
        WHERE ParameterID = 25671
          AND TextID = 1
    """,
)

# ---------------------------------------------------------------------------
# Master tables (lookups)
# ---------------------------------------------------------------------------

ICU_MASTER = Entity(
    name="icu_master",
    kind="master_from_sql",
    sqlite_table="icu_master",
    columns={
        "LogicalUnitID": "INTEGER PRIMARY KEY",
        "Text": "TEXT",
    },
    source_query="""
        SELECT DISTINCT LogicalUnitID, LogicalUnitName AS Text
        FROM DimLogicalUnit
        WHERE LogicalUnitID IN (1, 46, 48, 49, 50, 51, 68, 69)
    """,
    primary_key=["LogicalUnitID"],
)

ORSTATUS_MASTER = Entity(
    name="orstatus_master",
    kind="master_static",
    sqlite_table="orstatus_master",
    columns={
        "ORStatusID": "INTEGER PRIMARY KEY",
        "Text": "TEXT",
    },
    static_data=[
        (1, "Geplante Aufnahme"),
        (2, "Aufnahme"),
        (4, "Anaesthesie"),
        (6, "Verfügbar"),
        (7, "Geschlossen"),
    ],
    primary_key=["ORStatusID"],
)

G_CONSENT_MASTER = Entity(
    name="g_consent_master",
    kind="master_from_sql",
    sqlite_table="g_consent_master",
    columns={
        "TextID": "INTEGER PRIMARY KEY",
        "Text": "TEXT",
    },
    source_query="""
        SELECT DISTINCT TextID, Text
        FROM DimParametersText
        WHERE ParameterID = 10480
    """,
    primary_key=["TextID"],
)

IFI_CONSENT_MASTER = Entity(
    name="ifi_consent_master",
    kind="master_from_sql",
    sqlite_table="ifi_consent_master",
    columns={
        "TextID": "INTEGER PRIMARY KEY",
        "Text": "TEXT",
    },
    source_query="""
        SELECT DISTINCT TextID, Text
        FROM DimParametersText
        WHERE ParameterID = 25583
    """,
    primary_key=["TextID"],
)

# ---------------------------------------------------------------------------
# Transactional tables
# ---------------------------------------------------------------------------
PATIENTS = Entity(
    name="patients",
    kind="transactional",
    sqlite_table="patients",
    columns={
        "PatientID": "INTEGER PRIMARY KEY",
        "BirthDate": "TEXT",
        "LastName": "TEXT",
        "FirstName": "TEXT",
    },
    source_query="""
        SELECT PatientID, BirthDate, LastName, FirstName
        FROM dbo.DimPatient
    """,
    primary_key=["PatientID"],
)

ADMISSIONS = Entity(
    name="admissions",
    kind="transactional",
    sqlite_table="admissions",
    columns={
        "admission_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "PatientID": "INTEGER",
        "SocialSecurity": "TEXT",
        "AdmissionDate": "TEXT",
        "LogicalUnitID": "INTEGER",
        "BedID": "INTEGER",
        "ORStatus": "INTEGER",
        "LocationFromTime": "TEXT",
    },
    source_query="""
        SELECT
            PatientID,
            SocialSecurity,
            AddmissionDate,
            LogicalUnitID,
            BedID,
            ORStatus,
            LocationFromTime
        FROM dbo.DimPatient
    """,
    column_map={"AddmissionDate": "AdmissionDate"},
    foreign_keys=[
        "FOREIGN KEY (PatientID) REFERENCES patients(PatientID)",
        "FOREIGN KEY (LogicalUnitID) REFERENCES icu_master(LogicalUnitID)",
        "FOREIGN KEY (ORStatus) REFERENCES orstatus_master(ORStatusID)",
    ],
    unique_indexes=[["PatientID", "AdmissionDate"]],
    on_conflict="update",
)

G_CONSENT = Entity(
    name="g_consent",
    kind="transactional",
    sqlite_table="g_consent",
    columns={
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "PatientID": "INTEGER",
        "TimeStamp": "TEXT",
        "TextID": "INTEGER",
    },
    source_query="""
        SELECT PatientID, TimeStamp, TextID
        FROM dbo.FactSignal
        WHERE ParameterID = 10480
    """,
    foreign_keys=[
        "FOREIGN KEY (PatientID) REFERENCES patients(PatientID)",
        "FOREIGN KEY (TextID) REFERENCES g_consent_master(TextID)",
    ],
    unique_indexes=[["PatientID", "TimeStamp"]],
    incremental_column="TimeStamp",
)

IFI_CONSENT = Entity(
    name="ifi_consent",
    kind="transactional",
    sqlite_table="ifi_consent",
    columns={
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "PatientID": "INTEGER",
        "TimeStamp": "TEXT",
        "TextID": "INTEGER",
    },
    source_query="""
        SELECT PatientID, TimeStamp, TextID
        FROM dbo.FactSignal
        WHERE ParameterID = 25583
    """,
    foreign_keys=[
        "FOREIGN KEY (PatientID) REFERENCES patients(PatientID)",
        "FOREIGN KEY (TextID) REFERENCES ifi_consent_master(TextID)",
    ],
    unique_indexes=[["PatientID", "TimeStamp"]],
    incremental_column="TimeStamp",
)

GCS_SCORE = Entity(
    name="gcs_score",
    kind="transactional",
    sqlite_table="gcs_score",
    columns={
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "PatientID": "INTEGER",
        "TimeStamp": "TEXT",
        "Value": "INTEGER",
    },
    source_query="""
        SELECT PatientID, TimeStamp, Value
        FROM dbo.FactSignal
        WHERE ParameterID = 13736
    """,
    foreign_keys=["FOREIGN KEY (PatientID) REFERENCES patients(PatientID)"],
    unique_indexes=[["PatientID", "TimeStamp"]],
    incremental_column="TimeStamp",
)

# ---------------------------------------------------------------------------
# Local-only tables (no SQL Server source)
# ---------------------------------------------------------------------------
REGISTER = Entity(
    name="register",
    kind="local_only",
    sqlite_table="register",
    columns={
        "register_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "register_confirmed": "TEXT",
        "PatientID": "INTEGER",
    },
    foreign_keys=["FOREIGN KEY (PatientID) REFERENCES patients(PatientID)"],
    unique_indexes=[["PatientID"]],
)

SYNC_STATE = Entity(
    name="sync_state",
    kind="local_only",
    sqlite_table="sync_state",
    columns={
        "query_name": "TEXT PRIMARY KEY",
        "last_synced_at": "TEXT",
    },
)

# ---------------------------------------------------------------------------
# Registry — order matters: masters before tables that FK to them
# ---------------------------------------------------------------------------
ENTITIES: list[Entity] = [
    # filters (no SQLite table)
    TIME_FILTER,
    IFI_FILTER,
    TBI_FILTER,
    # masters first (FK targets)
    ICU_MASTER,
    ORSTATUS_MASTER,
    G_CONSENT_MASTER,
    IFI_CONSENT_MASTER,
    # transactional
    PATIENTS,
    ADMISSIONS,
    G_CONSENT,
    IFI_CONSENT,
    GCS_SCORE,
    # local-only
    REGISTER,
    SYNC_STATE,
]

ENTITIES_BY_NAME: dict[str, Entity] = {e.name: e for e in ENTITIES}


def entities_with_sqlite_table() -> list[Entity]:
    """Entities that produce a SQLite table (everything except filters)."""
    return [e for e in ENTITIES if e.sqlite_table is not None]


def transactional_entities() -> list[Entity]:
    return [e for e in ENTITIES if e.kind == "transactional"]


def master_entities() -> list[Entity]:
    return [e for e in ENTITIES if e.kind in ("master_from_sql", "master_static")]


def filter_entities() -> list[Entity]:
    return [e for e in ENTITIES if e.kind == "filter"]






