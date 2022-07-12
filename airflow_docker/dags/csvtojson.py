import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from elasticsearch import Elasticsearch

# with DAG('MyCSVDAG',
#         schedule_interval=timedelta(minutes=5),# '0 * * * *',
#         )as dag:

#     def csvtoJson():
#         df  = pd.read_csv("")
#         for i,r in df.iterrows:
#             print(r['name'])
#         df.to_json("",orient=records)
    
#     printing_bash = BashOperator(
#         task_id = "Hello bash",
#         bash_command = "echo 'hello world'"
#     )

#     csvtojson = PythonOperator(
#         task_id = "convertcsvtojson",
#         python_callable = csvtoJson
#     )

es = Elasticsearch(['http://127.0.0.1:9200'])
doc = {'name':'Leslie Sanchez','street':'1687 Hernandez Avenue Apt.','city':'South John','zip':'52010'}
res = es.index(index='users',doc_type="doc",body=doc)
print(res['result'])
