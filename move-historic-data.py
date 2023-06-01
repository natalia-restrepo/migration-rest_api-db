import pandas as pd
from functions import db_conection, insert_db, load_blob, valid_int, valid_none

# Create dataframe
df_departments = pd.DataFrame(load_blob("company","data/departments.csv"), columns=["id", "department"])
df_hired_employees = pd.DataFrame(load_blob("company","data/hired_employees.csv"), columns=["id", "name", "datetime", "department_id","job_id"])
df_jobs = pd.DataFrame(load_blob("company","data/jobs.csv"), columns=["id", "job"])

#Database conection
engine = db_conection(); 

# Reemplace empty values with None
#hired_employees
df_hired_employees_logs = valid_none(df_hired_employees)
#jobs
df_jobs_logs = valid_none(df_jobs)
#departments
df_departments_logs = valid_none(df_departments)

#valid int columns and sent them to the log in case of mismatch
#hired_employees
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'id')
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'department_id')
df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'job_id')
#jobs
df_jobs,df_jobs_logs = valid_int(df_jobs,df_jobs_logs,'id')
#departments
df_departments,df_departments_logs = valid_int(df_departments,df_departments_logs,'id')

#print(df_jobs_clean)
#print(df_departments_clean)
#print(df_hired_employees)
#print(df_hired_employees_logs)


# Insert df_jobs into the company.jobs table using the to_sql() method
insert_db(engine,df_jobs,'jobs','company','append')

# Insert df_departments into the company.departments table using the to_sql() method
insert_db(engine,df_departments,'departments','company','append')

# Insert df_hired_employees into the company.hired_employees table using the to_sql() method
insert_db(engine,df_hired_employees,'hired_employees','company','append')