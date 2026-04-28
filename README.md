# TBI

## Structure
### connection_tbi
This script is the data access layer — connecting, querying, and returning data from SQL database as pandas DataFrames
- 3 "filter" queries that return PatientIDs:
  - sql_consent — patients who gave consent (ParameterID 10480 = 1, OR 25583 = 2 or 4)
  - sql_ifi — ICU patients only
  - sql_tbi — patients with TBI diagnosis (ParameterID 25671 = 1)
- 5 "data" queries: 
  - sql_gender_master / sql_icu_master — lookup tables (decode IDs → text)
  - sql_gender — gender values per patient
  - sql_gcs — Glasgow Coma Scale measurements over time
  - sql_patient — demographic data (PatientID, SocialSecurita, AdmissionDate, LogicalUnitID, ORStatus, BedID, BirthDate, LastName, FirstName)

- def_handle_datetimeoffset: function to convert SQL's datetimeoffset (raw bytes) to Python datetime
- def get_connection: builds ODBC connection string and registers the datetime converter (type code -155 = SQL Server's datetimeoffset)
- def get_data: executes query and returns DataFrame

### sqlite_tbi
This script creates a local SQLite database with a schema for storing TBI dashboard data: 
- Master tables (lookups):
  - icu_master — decode LogicalUnitID → ICU name
  - gender_master — decode TextID → "Male"/"Female"
  - orstatus_master — decode ORStatusID → status text

- Transactional tables:
  - patients data
  - admissions — one patient can have multiple admissions
  - gcs_score — time-series GCS measurements
  - register — for tracking which patients have been "register_confirmed" into the Dashboard

- Metadata:
  - sync_state — tracks when each query was last synced from the live DB

- Unique indexes for deduplication:
  - ux_admissions_key — prevents duplicate admission rows
  - ux_gcs_score_key — prevents duplicate GCS measurements
  - ux_register_patient - prevents duplicates patients

### sqlite_masterdata_setup_tbi.py

### sync_tbi.py
### tbi_data.py
### tbi_register.py
### dashboard_tbi.py
### main_tbi.py

