"""
Connection to the SQL Live-Database for TBI.

This module is a pure data access layer:
  - builds the ODBC connection string from environment variables
  - converts SQL Server's datetimeoffset to Python datetime
  - runs queries and returns DataFrames

It does NOT know which queries exist — those live in schema.py.
To fetch an entity, pass the Entity object (or its source_query) to get_data().
"""
import os
import struct
from datetime import datetime

import pandas as pd
import pyodbc
from dotenv import load_dotenv

from schema import Entity, filter_entities

load_dotenv()

class TBIConnection:
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    def __init__(self):
        self.connection_string = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password};'
            f'TrustServerCertificate=yes;'
            f'Encrypt=yes;'
        )

    @staticmethod
    def _handle_datetimeoffset(dto_value):
        """
        converting raw datetimeoffset into python datetime
        :param dto_value: raw binary bytes
        :return: datetime
        """
        tup = struct.unpack("<6hI2h", dto_value)
        return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000)

    def get_connection(self):
        """
        remark: -155 is the ODBC type code for datetimeoffset
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            conn.add_output_converter(-155, self._handle_datetimeoffset)
            return conn
        except pyodbc.Error as e:
            print(f"Database connection error: {e}")
            return None

    def get_data(self, query: str, patient_ids=None) -> pd.DataFrame | None:
        """
        Execute a SQL query and return the results as a DataFrame.
        If patient_ids is given, wrap the query and restrict to those IDs.
        """
        conn = self.get_connection()
        if conn is None:
            return None
        try:
            if patient_ids is not None:
                ids = [int(pid) for pid in patient_ids]
                if not ids:
                    return pd.DataFrame()
                placeholders = ','.join(str(pid) for pid in ids)
                query = f"SELECT * FROM ({query}) AS sub WHERE PatientID IN ({placeholders})"

            cursor = conn.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame.from_records(rows, columns=columns)

        except pyodbc.Error as e:
            print(f"Query failed: {str(e)}")
            return None
        finally:
            conn.close()

    def fetch(self, entity: Entity, patient_ids=None) -> pd.DataFrame | None:
        """
        Fetch the rows for a schema Entity from SQL Server.
        Applies the entity's column_map so the returned DataFrame already
        uses the SQLite column names.
        """
        if entity.source_query is None:
            raise ValueError(f"Entity '{entity.name}' has no source_query")
        df = self.get_data(entity.source_query, patient_ids=patient_ids)
        if df is not None and entity.column_map:
            df = df.rename(columns=entity.column_map)
        return df

    def get_cohort(self) -> set[int]:
        """
        Run every filter in schema.filter_entities() and return the
        intersection of their PatientID sets. This is the cohort that
        every transactional sync should be restricted to.
        """
        cohort: set[int] | None = None
        for f in filter_entities():
            df = self.get_data(f.source_query)
            if df is None:
                print(f"  [cohort] filter '{f.name}' failed; skipping")
                continue
            ids = set(int(pid) for pid in df["PatientID"])
            print(f"  [cohort] {f.name}: {len(ids)} patients")
            cohort = ids if cohort is None else cohort & ids
        result = cohort or set()
        print(f"  [cohort] final: {len(result)} patients")
        return result


