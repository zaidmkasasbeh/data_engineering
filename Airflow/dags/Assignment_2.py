import subprocess
subprocess.check_call(['pip', 'install', 'matplotlib'])
subprocess.check_call(['pip', 'install', 'sklearn'])
import psycopg2
import matplotlib.pyplot as plt
import time
import pandas as pd
import csv
import datetime as dt
from sklearn.preprocessing import MinMaxScaler

from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator

List_of_days=[]
DF_all=[]
DF_UK_scaled=[]


def Get_DF_i(Day):
    DF_i=None
    try:
        URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(URL_Day)
        DF_day['Day']=Day
        cond=(DF_day.Country_Region=='United Kingdom')&(DF_day.Combined_Key=='England, United Kingdom')
        Selec_columns=['Day','Country_Region', 'Last_Update',
              'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
              'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
    except:
        pass
    return DF_i

def extract_from_github():
        for year in range(2020,2022):
            for month in range(1,13):
                for day in range(1,32):
                    month=int(month)
                    if day <=9:
                        day=f'0{day}'
                    if month <= 9 :
                        month=f'0{month}'
                        List_of_days.append(f'{month}-{day}-{year}')

        for Day in List_of_days:
            DF_all.append(Get_DF_i(Day))
        return DF_all

def feature_engineering_and_scaling():
    DF_UK=pd.concat(DF_all).reset_index(drop=True)
    # Create DateTime for Last_Update
    DF_UK['Last_Updat']=pd.to_datetime(DF_UK.Last_Update, infer_datetime_format=True)
    DF_UK['Day']=pd.to_datetime(DF_UK.Day, infer_datetime_format=True)
    DF_UK['Case_Fatality_Ratio']=DF_UK['Case_Fatality_Ratio'].astype(float)
    DF_UK_u=DF_UK.copy()
    DF_UK_u.index=DF_UK_u.Day
    Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_u_2=DF_UK_u[Selec_Columns]
    min_max_scaler = MinMaxScaler()
    DF_UK_u_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_UK_u_2[Selec_Columns]),columns=Selec_Columns)
    DF_UK_u_3.index=DF_UK_u_2.index
    DF_UK_u_3['Day']=DF_UK_u.Day
    DF_UK_scaled.append(DF_UK_u_3)

def reporting():
    font = {'weight' : 'bold', 'size'   : 18}
    matplotlib.rc('font', **font)
    DF_UK_scaled[Selec_Columns].plot(figsize=(20,10))
    plt.savefig('output/UK_scoring_report.png')
    DF_UK_scaled.to_csv('output/UK_scoring_report.csv')

def save_to_postgres():

    host="postgres" # use "localhost" if you access from outside the localnet docker-compose env
    database="airflow"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    for Day in List_of_days:
        Day = Day.replace("-","_")
        DF_UK_scaled.to_sql(f'UK_scoring_report_{Day}', engine,if_exists='replace',index=False)



default_args = { 'owner' :'Zaid',
                  'start_date': dt.datetime(2021,5,15),
                  'retries':2,
                  'execution_timeout':dt.timedelta(seconds=120),
                  'retry_delay': dt.timedelta(minutes=3)}

with DAG(dag_id='Assignment_2', default_args=default_args) as dag:

        extractFromGitHubAndSelection = PythonOperator(
            task_id="extractFromGitHubAndSelection",
            python_callable=extract_from_github
        )

        featureEngineeringAndScaling = PythonOperator(
            task_id="featureEngineeringAndScaling",
            python_callable=feature_engineering_and_scaling
        )

        reporting = PythonOperator(
            task_id="reporting",
            python_callable=reporting
        )

        saveDataToPostgreSQL = PythonOperator(
            task_id="saveDataToPostgreSQL",
            python_callable=save_to_postgres
        )

        extractFromGitHubAndSelection>>featureEngineeringAndScaling>>reporting>>saveDataToPostgreSQL
