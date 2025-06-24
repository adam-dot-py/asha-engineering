import pyodbc
import sqlalchemy as db
import pandas as pd

# Define the connection string
server = 'localhost,1401'
database = 'ashadev'
username = 'sa'
password = 'Adam123456'
driver = 'ODBC Driver 18 for SQL Server'

# connect
engine = db.create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=Yes")
connection = engine.connect()
metadata = db.MetaData()

# load the base table
base_tsm = db.Table('tsm_responses', 
                    metadata, 
                    autoload=True, 
                    autoload_with=engine,
                    future=True)

print(base_tsm.columns.keys())

