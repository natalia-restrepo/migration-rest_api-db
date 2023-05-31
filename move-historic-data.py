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

#Create a blob storage service client using an Azure AD credential
blob_service_client = BlobServiceClient(account_url="https://bscompany.blob.core.windows.net", credential=DefaultAzureCredential())

#Create a container client using the service client
container_client = blob_service_client.get_container_client("company")

#Create a blob client using the container client
blob_client_departments = container_client.get_blob_client("data/departments.csv")
blob_client_hired_employees = container_client.get_blob_client("data/hired_employees.csv")
blob_client_jobs = container_client.get_blob_client("data/jobs.csv")

#Download the content of the blob in a stream object
stream_departments = blob_client_departments.download_blob()
stream_hired_employees = blob_client_hired_employees.download_blob()
stream_jobs = blob_client_jobs.download_blob()

#Read the content of the stream as a csv file
reader_departments = csv.reader(stream_departments.readall().decode('utf-8').splitlines(),delimiter=",")
reader_hired_employees = csv.reader(stream_hired_employees.readall().decode('utf-8').splitlines(),delimiter=",")
reader_jobs = csv.reader(stream_jobs.readall().decode('utf-8').splitlines(),delimiter=",")

# Create dataframe
df_departments = pd.DataFrame(reader_departments, columns=["id", "department"])
df_hired_employees = pd.DataFrame(reader_hired_employees, columns=["id", "name", "datetime", "department_id","job_id"])
df_jobs = pd.DataFrame(reader_jobs, columns=["id", "job"])

# Create data base conection
server = os.environ["DBSERVER"]
database = os.environ["DBDATABASE"]
username = os.environ["DBUSERNAME"]
password = os.environ["DBPASSWORD"]
dbport = os.environ["DBPORT"]

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
except Exception as e:
    print("Conection fail:", e)
    exit()

# Reemplace empty values with None
#hired_employees
df_hired_employees = df_hired_employees.replace('', None)
# Create booleana mask to identify  empty values
mask_na = df_hired_employees.isna().any(axis=1)
# Create new dataframe with emtpty values
df_hired_employees_logs = df_hired_employees[mask_na]
#delete rows with empty values orginal dataframe
df_hired_employees = df_hired_employees[~mask_na]

#jobs
df_jobs = df_jobs.replace('', None)
# Create booleana mask to identify  empty values
mask_na = df_jobs.isna().any(axis=1)
# Create new dataframe with emtpty values
df_jobs_logs = df_jobs[mask_na]
#delete rows with empty values orginal dataframe
df_jobs = df_jobs[~mask_na]

#jobs
df_departments = df_departments.replace('', None)
# Create booleana mask to identify  empty values
mask_na = df_departments.isna().any(axis=1)
# Create new dataframe with emtpty values
df_departments_logs = df_departments[mask_na]
#delete rows with empty values orginal dataframe
df_departments = df_departments[~mask_na]

#valid int columns and sent them to the log in case of mismatch
#hired_employees
df_hired_employees['id_int'] = pd.to_numeric(df_hired_employees['id'], errors='coerce')
mask_numeric = df_hired_employees['id_int'].notna()
df_hired_employees = df_hired_employees[mask_numeric]
df_hired_employees.drop('id_int', axis=1, inplace=True)
df_hired_employees_logs = pd.concat([df_hired_employees_logs, df_hired_employees[~mask_numeric]])

df_hired_employees['department_id_int'] = pd.to_numeric(df_hired_employees['department_id'], errors='coerce')
mask_numeric = df_hired_employees['department_id_int'].notna()
df_hired_employees = df_hired_employees[mask_numeric]
df_hired_employees.drop('department_id_int', axis=1, inplace=True)
df_hired_employees_logs = pd.concat([df_hired_employees_logs, df_hired_employees[~mask_numeric]])

df_hired_employees['job_id_int'] = pd.to_numeric(df_hired_employees['job_id'], errors='coerce')
mask_numeric = df_hired_employees['job_id_int'].notna()
df_hired_employees = df_hired_employees[mask_numeric]
df_hired_employees.drop('job_id_int', axis=1, inplace=True)
df_hired_employees_logs = pd.concat([df_hired_employees_logs, df_hired_employees[~mask_numeric]])

#jobs
df_jobs['id_int'] = pd.to_numeric(df_jobs['id'], errors='coerce')
mask_numeric = df_jobs['id_int'].notna()
df_jobs = df_jobs[mask_numeric]
df_jobs.drop('id_int', axis=1, inplace=True)
df_jobs_logs = pd.concat([df_jobs_logs, df_jobs[~mask_numeric]])

#departments
df_departments['id_int'] = pd.to_numeric(df_jobs['id'], errors='coerce')
mask_numeric = df_departments['id_int'].notna()
df_departments = df_departments[mask_numeric]
df_departments.drop('id_int', axis=1, inplace=True)
df_departments_logs = pd.concat([df_departments_logs, df_departments[~mask_numeric]])

# Insert df_jobs into the company.jobs table using the to_sql() method
df_jobs.to_sql('jobs', con=engine, schema='company', if_exists='append', index=False)
print("LOAD jobs TABLE, rows inserted: " , len(df_jobs.index))

# Insert df_departments into the company.departments table using the to_sql() method
df_departments.to_sql('departments', con=engine, schema='company', if_exists='append', index=False)
print("LOAD departments TABLE, rows inserted: " , len(df_departments.index))

# Insert df_hired_employees into the company.hired_employees table using the to_sql() method
df_hired_employees.to_sql('hired_employees', con=engine, schema='company', if_exists='append', index=False)
print("LOAD hired_employees TABLE, rows inserted: " , len(df_hired_employees.index))