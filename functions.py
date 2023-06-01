import csv
import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.url import URL

# Load env variables from file
load_dotenv()

#Access environment variables using os.environ
client_id = os.environ["AZURE_CLIENT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]
tenant_id = os.environ["AZURE_TENANT_ID"]
account_url = os.environ["ACCOUNT_URL"]

#Access  enviaroment variables data base conection
server = os.environ["DBSERVER"]
database = os.environ["DBDATABASE"]
username = os.environ["DBUSERNAME"]
password = os.environ["DBPASSWORD"]
dbport = os.environ["DBPORT"]

# Define una función para descargar un blob
def load_blob(container_name:str, blob_name:str):
    #Create a blob storage service client using an Azure AD credential
    blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    #Create a container client using the service client
    container_client = blob_service_client.get_container_client(container_name)
    #Create a blob client using the container client
    blob_client = container_client.get_blob_client(blob_name)
    # load content of blob on stream object
    stream = blob_client.download_blob()
    #Read the content of the stream as a csv file
    reader = csv.reader(stream.readall().decode('utf-8').splitlines(),delimiter=",")
    # return el reader
    return reader

def db_conection ():
    # Create URL with param of the conetion
    db_url = URL.create(
        drivername="mssql+pyodbc",
        username=username,
        password=password,
        host=server,
        port=dbport,
        database=database,
        query={"driver": "ODBC Driver 18 for SQL Server"}
    )

    try:
        # create conection to DB using sqlalchemy
        engine = sqlalchemy.create_engine(db_url, fast_executemany=True)
        print("Conection success")
        return engine
    except Exception as e:
        print("Conection fail:", e)
        return ""
        exit()

def valid_none(df:pd.DataFrame):
    # Reemplace empty values with None
    df = df.replace('', None)
    # Create booleana mask to identify  empty values
    mask_na = df.isna().any(axis=1)
    # Create new dataframe with emtpty values
    df_logs = df[mask_na]
    #delete rows with empty values orginal dataframe
    df = df[~mask_na]
    return df_logs

#valid int columns and sent them to the log in case of mismatch
def valid_int(df:pd.DataFrame,df_logs:pd.DataFrame,name_column:str): 
    df[name_column + '_int'] = pd.to_numeric(df[name_column], errors='coerce')
    mask_numeric = df[name_column + '_int'].notna()
    df_clean = df[mask_numeric].copy() 
    df_clean.drop(name_column + '_int', axis=1, inplace=True) 
    df_logs_int = pd.concat([df_logs, df[~mask_numeric]]) 
    df_logs_int.drop(name_column + '_int', axis=1, inplace=True) 
    return df_clean, df_logs_int

# Insert df_jobs into the company.jobs table using the to_sql() method
def insert_db(engine,df:pd.DataFrame,name_table:str,schema_name:str,operation:str):    
    df.to_sql(name_table, con=engine, schema=schema_name, if_exists=operation, index=False)
    print("LOAD ", name_table, " TABLE, rows inserted: " , len(df.index))