from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import requests
import json

with DAG(
        dag_id="currency_etl",
        start_date=datetime(2024, 1, 1, 9),
        schedule="@monthly",
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": 3,
            "retry_delay": timedelta(minutes=5)
      }
) as dag:
    # Create a task using the TaskFlow API
        @task()
        def hit_currency_api(**context):
            token = "cur_live_NB9zFqtbV6mVtum8uOlS1bZsg7cVMdqBwacqXQZW"
            #Remove the '@' at the end of the token in real run
            #WARNING!!! There is limit for api hit !!!WARNING
            deployment_url = f"https://api.currencyapi.com/v3/latest?apikey={token}"
            print(deployment_url)
            response = requests.get(
                url=deployment_url
            )
            return response.json() 
        
        @task
        def flatten_market_data(api_response, **context):
            # Create a list to append the data to

            last_updated_at = api_response['meta']['last_updated_at']
            currency_list = api_response['data']
            rows_list = []

            for currency in currency_list:
                    dict1 = (currency_list[currency])
                    dict1.update({'last_updated_at': last_updated_at})
                    rows_list.append(dict1)

            df = pd.DataFrame(rows_list) 
            return df
        
        @task
        def load_market_data(flattened_dataframe):
            pg_hook = PostgresHook(postgres_conn_id='postgres_local', schema='currency')
            print(pg_hook.get_sqlalchemy_engine())
            flattened_dataframe.to_sql('currency_table', pg_hook.get_sqlalchemy_engine(), if_exists='append', index=False)

        api_response = hit_currency_api()
        load_market_data(flatten_market_data(api_response))             
    

# Prints data about all DAGs in your Deployment