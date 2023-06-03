# migration-csv-db

migration data using REST API to the SQL Server database

1\. Move historic data from files in CSV format to the new database.

  py move-historic-data.py

  Read data from Blob storage and load into database

2\. Create a Rest API service to receive new data. This service must have:

2.1. Each new transaction must fit the data dictionary rules.

2.2. Be able to insert batch transactions (1 up to 1000 rows) with one request.

2.3. Receive the data for each table in the same service.

2.4. Keep in mind the data rules for each table.

main.py

Create Rest API using fastAPI framework:

  Create env

  virtualenv -p python path\virtual

  activate

  .\activate.ps1

exec app

uvicorn main:app --reload

Access url to test API:

  http://127.0.0.1:8000/

  http://127.0.0.1:8000/docs
