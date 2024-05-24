from airflow.decorators import task, dag
import json

@dag(
    schedule=None,
    catchup=False,
    tags=['dotty']

)

def myfirstdag():

    @task
    def extract():

        data = {'carlos': 3, 'dots': 6, 'lulu': 2, 'gigi': 2}

        data_dict = json.loads(data)
        print(data_dict)
        
        return data_dict
    
    @task
    def transform(data_dict):

        total_age = 0

        for v in data_dict.values():
            total_age += v

        return total_age
    
    @task
    def load(total_age):

        print(total_age)

    
    create_bunnies = extract()
    sum_ages = transform(create_bunnies)
    load(sum_ages['total_age'])

myfirstdag()



