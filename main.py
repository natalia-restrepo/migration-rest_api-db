import datetime
from fastapi import  FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from functions import db_conection, insert_db, load_log, valid_int, valid_none, valid_unique

app = FastAPI()

#tables model
class Hired_Employees(BaseModel):
    id : Optional[str]
    name : Optional[str]
    datetime : Optional[str]    
    department_id : Optional[str]
    job_id : Optional[str]

class Departments(BaseModel):
    id : Optional[str]
    department : Optional[str]

class Jobs(BaseModel):
    id : Optional[str]
    job : Optional[str]


@app.get('/')
def read_root():
    return {"welcome":"Welcome to my REST API"}


@app.post("/hired_employees")
def load_hired_employees(hired_employees: List[Hired_Employees]):
    try:
        #Database conection
        engine = db_conection()
        df_hired_employees = pd.DataFrame([t.dict() for t in hired_employees])
        # Reemplace empty values with None
        df_hired_employees,df_hired_employees_logs = valid_none(df_hired_employees)    
        #valid int
        df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'id')
        df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'department_id')
        df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'job_id')
        #valid unique        
        df_hired_employees,df_hired_employees_logs = valid_unique(engine,df_hired_employees,df_hired_employees_logs,'company','hired_employees',["id", "name","datetime","department_id","job_id"])        
        # Insert df_hired_employees into the company.hired_employees table using the to_sql() method
        insert_db(engine,df_hired_employees,'hired_employees','company','append')
        #insert log table
        if not  df_hired_employees_logs.empty:
            load_log(engine, df_hired_employees_logs,["id", "name","datetime","department_id","job_id"],'hired_employees','company','append')
        return hired_employees
    except Exception as e:
        raise HTTPException (status_code = 400, detail = str (e))
    
@app.post("/jobs")
def load_jobs(jobs: List[Jobs]):
    try:
        #Database conection
        engine = db_conection()
        df_jobs = pd.DataFrame([t.dict() for t in jobs])
        # Reemplace empty val
        # ues with None
        #valid empty values
        df_jobs,df_jobs_logs = valid_none(df_jobs)    
        #valid int
        df_jobs,df_jobs_logs = valid_int(df_jobs,df_jobs_logs,'id')
        #valid unique
        df_jobs,df_jobs_logs = valid_unique(engine,df_jobs,df_jobs_logs,'company','jobs',["id", "job"])
        # Insert df_hired_employees into the company.hired_employees table using the to_sql() method
        insert_db(engine,df_jobs,'jobs','company','append')
        #insert log table
        if not df_jobs_logs.empty:
            load_log(engine,df_jobs_logs,["id", "job"],'jobs','company','append')
        return jobs
    except Exception as e:
        raise HTTPException (status_code = 400, detail = str (e))

@app.post("/departments")
def load_deparments(deparments: List[Departments]):
    try:
        #Database conection
        engine = db_conection()
        df_departments = pd.DataFrame([t.dict() for t in deparments])
        # Reemplace empty values with None
        #valid empty value
        df_departments,df_departments_logs = valid_none(df_departments)    
        #valid int
        df_departments,df_departments_logs = valid_int(df_departments,df_departments_logs,'id')
        #valid unique
        df_departments,df_departments_logs = valid_unique(engine,df_departments,df_departments_logs,'company','departments',['id','department'])        
        # Insert df_hired_employees into the company.hired_employees table using the to_sql() method
        insert_db(engine,df_departments,'deparments','company','append')
        #insert log table
        if not df_departments_logs.empty:
            load_log(engine,df_departments_logs,["id", "department"],'departments','company','append')        
        return df_departments
    except Exception as e:
        raise HTTPException (status_code = 400, detail = str (e))