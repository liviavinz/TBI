"""
This script contains the connection to the SQL Live-Database and all SQL-Queries for TBI
"""
import pyodbc
import pandas as pd
import os
import struct
from datetime import datetime
from tabulate import tabulate
from dotenv import load_dotenv

load_dotenv()

class TBIConnection:
    """
    - Server Information
    - SQL Queries:
        - predefine consent, IFI and TBI only
        - master data (gender, icu)
        - transactional data (gender, gcs, patient data)
    - Functions to make connection and load data
    """
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    sql_consent = '''
        SELECT DISTINCT PatientID
        FROM dbo.FactSignal
        WHERE (ParameterID = 10480 AND TextID IN (1))
            OR (ParameterID = 25583 AND TextID IN (2, 4))
    '''
    sql_ifi = """
        SELECT DISTINCT PatientID
        FROM dbo.DimPatient
        WHERE LogicalUnitID IN (1, 46, 48, 49, 50, 51, 68, 69)
    """
    sql_tbi = """
        SELECT DISTINCT PatientID
        FROM dbo.FactSignal
        WHERE ParameterID = 25671
        AND TextID = 1
    """

    sql_gender_master = """SELECT DISTINCT TextID, Text FROM DimParametersText WHERE ParameterID = 3747"""
    sql_icu_master = """SELECT DISTINCT LogicalUnitID, LogicalUnitName AS Text FROM DimLogicalUnit
                        WHERE LogicalUnitID IN (1, 46, 48, 49, 50, 51, 68, 69)"""

    sql_gender = """SELECT PatientID, Text, TextID FROM dbo.FactSignal WHERE ParameterID = 3747"""
    sql_gcs = """SELECT PatientID, TimeStamp, Value FROM dbo.FactSignal WHERE ParameterID = 13736"""
    sql_patient = """
        SELECT
            PatientID,
            SocialSecurity,
            AddmissionDate,
            LogicalUnitID,
            ORStatus,
            BedID,
            BirthDate,
            LastName,
            FirstName,
            LocationFromTime
        FROM dbo.DimPatient
    """

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

    def get_data(self, query, patient_ids=None):
        """
        Execute SQL query and return results as a DataFrame
        :param query: SQL query string
        :param patient_ids: iterable of PatientIDs to filter by
        :return: pandas DataFrame or None on failure
        """
        conn = self.get_connection()
        if conn is None:
            return None
        try:
            if patient_ids:
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


