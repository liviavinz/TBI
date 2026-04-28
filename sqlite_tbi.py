"""
This script creates SQLite Database and tables
"""

# DB_PATH = '/home/vinzl/Documents/tbi_database.sqlite'
# import os
# os.remove(DB_PATH)
import sqlite3
import pandas as pd

class TBIDatabase:
    """
    - SQLite create Tables
    - functions to connect with DB SQlite, create tables and queries
    """
    DB_PATH = '/home/vinzl/Documents/tbi_database.sqlite'

    _CREATE_TABLES = '''
            CREATE TABLE IF NOT EXISTS icu_master (
                LogicalUnitID INTEGER PRIMARY KEY,
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS gender_master (
                TextID INTEGER PRIMARY KEY,
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS orstatus_master (
                ORStatusID INTEGER PRIMARY KEY,
                Text TEXT
                );
            CREATE TABLE IF NOT EXISTS patients (
                PatientID INTEGER PRIMARY KEY,
                BirthDate INTEGER,
                LastName TEXT,
                FirstName TEXT,
                gender_TextID INTEGER,
                FOREIGN KEY (gender_TextID) REFERENCES gender_master(TextID)
                );
            CREATE TABLE IF NOT EXISTS admissions (
                admission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                PatientID INTEGER,
                SocialSecurity TEXT,
                LogicalUnitID INTEGER,
                AdmissionDate TEXT,
                ORStatus INTEGER,
                BedID INTEGER,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID),
                FOREIGN KEY (ORStatus) REFERENCES orstatus_master(ORStatusID),
                FOREIGN KEY (LogicalUnitID) REFERENCES icu_master(LogicalUnitID)
                );
            CREATE TABLE IF NOT EXISTS gcs_score (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                PatientID INTEGER,
                TimeStamp TEXT,
                Value INTEGER,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID)
                );
            CREATE TABLE IF NOT EXISTS register (
                register_id INTEGER PRIMARY KEY AUTOINCREMENT,
                register_confirmed TEXT DEFAULT '',
                PatientID INTEGER,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID)
                );
            CREATE TABLE IF NOT EXISTS sync_state (
                query_name TEXT PRIMARY KEY,
                last_synced_at TEXT
                );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_admissions_key
            ON admissions (PatientID, SocialSecurity, LogicalUnitID, AdmissionDate);

            CREATE UNIQUE INDEX IF NOT EXISTS ux_gcs_score_key
            ON gcs_score (PatientID, TimeStamp, Value);
        '''

    def get_connection(self):
        conn = sqlite3.connect(self.DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def create_tables(self):
        conn = self.get_connection()
        conn.cursor().executescript(self._CREATE_TABLES)
        conn.commit()
        conn.close()

    def query(self, sql: str):
        with self.get_connection() as conn:
            return pd.read_sql(sql, conn)

