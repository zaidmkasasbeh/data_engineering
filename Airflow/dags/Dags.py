import datetime as dt
import pandas as pd
import csv
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import subprocess
subprocess.check_call(['pip', 'install', 'pymongo'])
subprocess.check_call(['pip', 'install', 'faker'])
from faker import Faker
from pymongo import MongoClient

host="postgres" # use "localhost" if you access from outside the localnet docker-compose env
database="airflow"
user="airflow"
password="airflow"
port='5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

client = MongoClient('mongo:27017',
                        username='root',
                         password='example')

def postgresSaveData():
 output=open('data.csv','w')
 fake=Faker()
 header=['name','age','street','city','state','zip','lng','lat']
 mywriter=csv.writer(output)
 mywriter.writerow(header)
 for r in range(10000):
    row =[fake.name(),fake.random_int(min=18,max=80, step=1),
                       fake.street_address(), fake.city(),fake.state(),
                       fake.zipcode(),fake.longitude(),fake.latitude()]
    mywriter.writerow(row)
 output.close()
 DF.to_sql('users2020', engine, if_exists='replace',index=False)

def csvToJson():
    df = pd.read_csv('data.csv')
    df.to_json('data.json', orient='records')

def saveJsonToMongoDB():
    db = client['de_assignment_1']
    articles = db.random
    f = open('data.json',)
    data = json.load(f)
    result = articles.insert_many(data)

default_args = { 'owner' :'Zaid',
                  'start_date': dt.datetime(2021,5,15),
                  'retries':1,
                  'retry_delay': dt.timedelta(minutes=5)}

with DAG(dag_id='dag', default_args=default_args) as dag:
        postgresGenerateDataThenSaveToCSV = PythonOperator(
            task_id="postgres_Generate_Data_Then_Save_To_CSV",
            python_callable=postgresSaveData
        )

        CSV_to_Json = PythonOperator(
            task_id="CSV_to_Json",
            python_callable=csvToJson
        )

        saveJsonFileToMongoDB = PythonOperator(
            task_id="save_Json_File_To_MongoDB",
            python_callable=saveJsonToMongoDB
        )

        postgresGenerateDataThenSaveToCSV>>CSV_to_Json>>saveJsonFileToMongoDB
