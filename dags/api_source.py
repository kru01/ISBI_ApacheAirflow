from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import requests

server = 'host.docker.internal'
database = 'ap_airflow'
username = 'sa'
password = '12345'

# Create the connection string for pymssql
connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
engine = create_engine(connection_string)

def get_LSET(table_name):
        query = f"""
            SELECT LSET FROM metadata
            WHERE TABNAME = '{table_name}'
        """
        LSET = pd.read_sql(query, engine)
        return (LSET['LSET'][0]).date()
def update_LSET(table_name, LSET):
    query = f"""
        UPDATE metadata
        SET LSET = '{LSET}'
        WHERE TABNAME = '{table_name}'
    """
    with engine.connect() as connection:
        connection.execute(query)
def truncate_table(table_name):
    query = f'TRUNCATE TABLE {table_name}'
    with engine.connect() as connection:
        connection.execute(query)

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
            CET = date.today()
            LSET = get_LSET('S_currency')

            truncate_table('S_currency')

            currency_df['LastUpdated'] = pd.to_datetime(currency_df['LastUpdated']).dt.date
            currency_df = currency_df[(currency_df['LastUpdated'] > LSET) & (currency_df['LastUpdated'] <= CET)]
            print(CET)
            print(LSET)
            # Insert new records into the staging table
            if not currency_df.empty:
                ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
                errors_list = []
                with engine.connect() as connection:
                    for index, row in currency_df.iterrows():
                        try:
                            check_query = f"""
                                SELECT * FROM S_currency WHERE code='{row['code']}'
                            """
                            current_currency = connection.execute(check_query).scalar_one_or_none()
                            print(current_currency)
                            if current_currency:
                                update_query = f"""
                                    UPDATE S_currency
                                    SET ConvertValue={row['value']}, LastUpdated='{row['LastUpdated']}'
                                    WHERE code='{row['code']}'
                                """
                                connection.execute(update_query)
                            else:
                                insert_query = f"""
                                    INSERT INTO S_currency 
                                    VALUES ('{row['code']}',{row['value']}, '{row['LastUpdated']}', '{row['LastUpdated']}')
                                """
                                connection.execute(insert_query)
                        except Exception as e:
                            print(f"An error occurred while inserting row {row['row_id']}: {e}")
                            err = {
                                'code': row['code'],
                                'ConvertValue': row['value'],
                                'error': str(e),
                                'LastUpdated': CET
                            }
                            errors_list.append(err)

                error_df = pd.DataFrame(errors_list)
                error_df.to_sql('currency_error_logs', ps_engine, if_exists='append', index=False) 
            update_LSET('S_currency', CET)

        api_response = hit_currency_api()
        load_currency_data(flatten_market_data(api_response))             
    

# Prints data about all DAGs in your Deployment