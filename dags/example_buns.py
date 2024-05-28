from airflow.decorators import task, dag
import json
from pendulum import datetime
from airflow.operators.python import get_current_context

#Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Rosie"},
    tags=["charlie"],
)
def myfirstdag():

    @task() 
    def extract():

        data = '{"carlos": 3, "dots": 6, "lulu": 2, "gigi": 2}'

        data_dict = json.loads(data)
        print(data_dict)
        
        return data_dict

    @task()
    def transform(data_dict):
        
        total_age = 0

        for v in data_dict.values():
            total_age += v

        return total_age

    @task()
    def load(total_age):

        for k, v in get_current_context().items():
            print(f"{k}: {v}")

        print(total_age)

    create_bunnies = extract()
    sum_ages = transform(create_bunnies)
    load(sum_ages)

#Instantiate the DAG
myfirstdag()