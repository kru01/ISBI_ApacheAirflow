from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import requests

with DAG(
        dag_id="api_source",
        start_date=datetime(2024, 1, 1, 9),
        schedule='0 6 * * *',
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

            cnt = 1
            for currency in currency_list:
                    dict1 = (currency_list[currency])
                    dict1.update({'LastUpdated': last_updated_at})
                    dict1.update({'row_id': cnt})
                    rows_list.append(dict1)
                    cnt += 1

            df = pd.DataFrame(rows_list) 
            return df
        
        @task
        def load_currency_data(currency_df):
            today = date.today()
            server = 'host.docker.internal'
            database = 'ap_airflow'
            username = 'sa'
            password = '12345'

            # Create the connection string for pymssql
            connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
            engine = create_engine(connection_string)

            currency_df['LastUpdated'] = pd.to_datetime(currency_df['LastUpdated']).dt.date
            with engine.connect() as connection:
                latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM currency"
                latest_timestamp = connection.execute(latest_timestamp_query).scalar()
            new_records = currency_df[currency_df['LastUpdated'] > latest_timestamp]

            # Insert new records into the staging table
            if not new_records.empty:
                ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
                errors_list = []
                with engine.connect() as connection:
                    for index, row in new_records.iterrows():
                        try:
                            check_query = f"""
                                SELECT * FROM currency WHERE code='{row['code']}'
                            """
                            current_currency = connection.execute(check_query).scalar_one_or_none()
                            print(current_currency)
                            if current_currency:
                                update_query = f"""
                                    UPDATE currency
                                    SET conv_value={row['value']}, LastUpdated='{row['LastUpdated']}'
                                    WHERE code='{row['code']}'
                                """
                                connection.execute(update_query)
                            else:
                                insert_query = f"""
                                    INSERT INTO currency 
                                    VALUES ('{row['code']}',{row['value']},'{row['LastUpdated']}')
                                """
                                connection.execute(insert_query)
                        except Exception as e:
                            print(f"An error occurred while inserting row {row['row_id']}: {e}")
                            err = {
                                'code': row['code'],
                                'conv_value': row['value'],
                                'error': str(e),
                                'LastUpdated': today
                            }
                            errors_list.append(err)

                error_df = pd.DataFrame(errors_list)
                error_df.to_sql('currency_error_logs', ps_engine, if_exists='append', index=False) 
            

        api_response = hit_currency_api()
        load_currency_data(flatten_market_data(api_response))             
    

# Prints data about all DAGs in your Deployment