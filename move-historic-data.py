import pandas as pd
from functions import db_conection, insert_db, load_blob, load_log, valid_int, valid_none, valid_unique

# Create dataframe
df_departments = pd.DataFrame(load_blob("company","data/departments.csv"), columns=["id", "department"])
df_hired_employees = pd.DataFrame(load_blob("company","data/hired_employees.csv"), columns=["id", "name", "datetime", "department_id","job_id"])
df_jobs = pd.DataFrame(load_blob("company","data/jobs.csv"), columns=["id", "job"])

#Database conection
engine = db_conection(); 

# Reemplace empty values with None
#hired_employees
df_hired_employees,df_hired_employees_logs = valid_none(df_hired_employees)
#jobs
df_jobs,df_jobs_logs = valid_none(df_jobs)
#departments
df_departments,df_departments_logs = valid_none(df_departments)

#valid int columns and sent them to the log in case of mismatch
#hired_employees
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'id')
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'department_id')
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'job_id')
#jobs
df_jobs,df_jobs_logs = valid_int(df_jobs,df_jobs_logs,'id')
#departments
df_departments,df_departments_logs = valid_int(df_departments,df_departments_logs,'id')

#valid unique
#hired_employees
df_hired_employees,df_hired_employees_logs = valid_unique(engine,df_hired_employees,df_hired_employees_logs,'company','hired_employees',["id", "name","datetime","department_id","job_id"])
#jobs
df_jobs,df_jobs_logs = valid_unique(engine,df_jobs,df_jobs_logs,'company','jobs',["id", "job"])
#departments
df_departments,df_departments_logs = valid_unique(engine,df_departments,df_departments_logs,'company','departments',['id','department'])


# Insert df_jobs into the company.jobs table using the to_sql() method
insert_db(engine,df_jobs,'jobs','company','append')

# Insert df_departments into the company.departments table using the to_sql() method
insert_db(engine,df_departments,'departments','company','append')

# Insert df_hired_employees into the company.hired_employees table using the to_sql() method
insert_db(engine,df_hired_employees,'hired_employees','company','append')

#insert log table
if not df_jobs_logs.empty:
    load_log(engine,df_jobs_logs,["id", "job"],'jobs','company','append')
if not df_departments_logs.empty:
    load_log(engine,df_departments_logs,["id", "department"],'departments','company','append')
if not  df_hired_employees_logs.empty:
    load_log(engine, df_hired_employees_logs,["id", "name","datetime","department_id","job_id"],'hired_employees','company','append')

engine.dispose()