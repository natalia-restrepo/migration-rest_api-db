from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd

from functions import db_conection, insert_db, valid_int, valid_none
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

class hired_employees(BaseModel):
    id : Optional[str]
    job : Optional[str]


@app.get('/')
def read_root():
    return {"welcome":"Welcome to my REST API"}


@app.get('/posts')
def get_post():
    return "posts"

@app.post("/hired_employees")
def create_transactions(hired_employees: List[Hired_Employees]):
    #Database conection
    engine = db_conection(); 
    df_hired_employees = pd.DataFrame([t.dict() for t in hired_employees])
    # Reemplace empty values with None
    #hired_employees
    df_hired_employees_logs = valid_none(df_hired_employees)    
    #hired_employees
    df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'id')
    df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'department_id')
    df_hired_employees,df_hired_employees_logs = valid_int(df_hired_employees,df_hired_employees_logs,'job_id')
    # Insert df_hired_employees into the company.hired_employees table using the to_sql() method
    insert_db(engine,df_hired_employees,'hired_employees','company','append')
    return hired_employees