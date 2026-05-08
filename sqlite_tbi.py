"""
This script creates SQLite Database and tables
"""
import os
import sqlite3
import pandas as pd
from pathlib import Path

class TBIDatabase:
    """
    - SQLite create Tables
    - functions to connect with DB SQlite, create tables and queries
    """
    DB_PATH = os.getenv(
        "SQLITE_DB_PATH",
        str(Path(__file__).parent / "data" / "tbi_database.sqlite")
    )

    _CREATE_TABLES = '''
            CREATE TABLE IF NOT EXISTS icu_master (
                LogicalUnitID INTEGER PRIMARY KEY,
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS orstatus_master (
                ORStatusID INTEGER PRIMARY KEY,
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS g_consent_master (
                TextID INTEGER PRIMARY KEY, 
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS ifi_consent_master (
                TextID INTEGER PRIMARY KEY, 
                Text TEXT
                );
                
                
            CREATE TABLE IF NOT EXISTS patients (
                PatientID INTEGER PRIMARY KEY,
                BirthDate TEXT,
                LastName TEXT,
                FirstName TEXT,
                );
            CREATE TABLE IF NOT EXISTS admissions (
                admission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                PatientID INTEGER,
                SocialSecurity TEXT,
                AdmissionDate TEXT,
                LogicalUnitID INTEGER,
                BedID INTEGER,
                ORStatus INTEGER,
                LocationFromTime TEXT,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID),
                FOREIGN KEY (LogicalUnitID) REFERENCES icu_master(LogicalUnitID),
                FOREIGN KEY (ORStatus) REFERENCES orstatus_master(ORStatusID)
                );
            
            CREATE TABLE IF NOT EXISTS g_constent(
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                PatientID INTEGER, 
                TimeStamp TEXT, 
                TextID INTEGER, 
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID),
                FOREIGN KEY (TextID) REFERENCES g_consent_,aster(TextID)
                FOREIGN KEY (
            
            CREATE TABLE IF NOT EXISTS gcs_score (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                PatientID INTEGER,
                TimeStamp TEXT,
                Value INTEGER,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID)
                );
                
            CREATE TABLE IF NOT EXISTS register (
                register_id INTEGER PRIMARY KEY AUTOINCREMENT,
                register_confirmed TEXT,
                PatientID INTEGER,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID)
                );
            CREATE TABLE IF NOT EXISTS sync_state (
                query_name TEXT PRIMARY KEY,
                last_synced_at TEXT
                );
                
            
            CREATE UNIQUE INDEX IF NOT EXISTS ux_gcs_score_key
            ON gcs_score (PatientID, TimeStamp);
            
            CREATE UNIQUE INDEX IF NOT EXISTS ux_admissions_key
            ON admissions (PatientID, AdmissionDate);
            
            CREATE UNIQUE INDEX IF NOT EXISTS ux_register_patient
            ON register (PatientID);
        '''

    def get_connection(self):
        Path(self.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def create_tables(self):
        with self.get_connection() as conn:
            conn.executescript(self._CREATE_TABLES)

    def query(self, sql: str):
        with self.get_connection() as conn:
            return pd.read_sql(sql, conn)

