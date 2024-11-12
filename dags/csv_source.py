from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pandas as pd

server = 'host.docker.internal'
database = 'ap_airflow'
username = 'sa'
password = '12345'

# Create the connection string for pymssql
connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
mssql_engine = create_engine(connection_string)

with DAG(
      dag_id="csv_source",
      start_date=datetime(2024, 1, 1, 9),
      schedule='0 8 * * *',
      catchup=False,
      max_active_runs=1,
      default_args={
      "retries": 3,
      "retry_delay": timedelta(minutes=5)
      }
) as dag:
    def get_LSET(table_name):
        query = f"""
            SELECT LSET FROM metadata
            WHERE TABNAME = '{table_name}'
        """
        LSET = pd.read_sql(query, mssql_engine)
        return (LSET['LSET'][0]).date()
    def update_LSET(table_name, LSET):
        query = f"""
            UPDATE metadata
            SET LSET = '{LSET}'
            WHERE TABNAME = '{table_name}'
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)
    def truncate_table(table_name):
        query = f'TRUNCATE TABLE {table_name}'
        with mssql_engine.connect() as connection:
            connection.execute(query)

    @task
    def insert_country_currency():
        CET = date.today()
        LSET = get_LSET('S_country_currency')

        truncate_table('S_country_currency')

        country_currency_df = pd.read_csv('source_data/country_currency.csv')
        country_currency_df['Created'] = pd.to_datetime(country_currency_df['Created']).dt.date
        country_currency_df['LastUpdated'] = pd.to_datetime(country_currency_df['LastUpdated']).dt.date
        print(CET)
        print(LSET)
        #Incremental update, LSET < Created <= CET or LSET < LastUpdated <= CET
        country_currency_df = country_currency_df[((country_currency_df['Created'] > LSET) & (country_currency_df['Created'] <= CET))
        | ((country_currency_df['LastUpdated'] > LSET) & (country_currency_df['LastUpdated'] <= CET))]
        
        # Insert new records into the staging table
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        errors_list = []
        with mssql_engine.connect() as connection:
            for index, row in country_currency_df.iterrows():
                try:
                    query = f"""
                        INSERT INTO S_country_currency 
                        VALUES ({row['id']}, '{row['eng_name']}', '{row['iso']}', '{row['iso3']}', '{row['dial']}', '{row['currency']}', '{CET}', '{CET}')
                    """
                    connection.execute(query)
                except Exception as e:
                    print(f"An error occurred while inserting row: {e},", query)
                    err = {
                        'type': 'country_currency',
                        'query_string': str(query),
                        'Date': CET
                    }
                    errors_list.append(err)

            error_df = pd.DataFrame(errors_list)
            error_df.to_sql('error_log_general', ps_engine, if_exists='append', index=False) 
        update_LSET('S_country_currency', CET)
    @task
    def insert_customer():
        CET = date.today()
        LSET = get_LSET('S_customer')

        truncate_table('S_customer')

        customer_df = pd.read_csv('source_data/customer.csv')
        customer_df['Created'] = pd.to_datetime(customer_df['Created']).dt.date
        customer_df['LastUpdated'] = pd.to_datetime(customer_df['LastUpdated']).dt.date
        print(CET)
        print(LSET)
        #Incremental update, LSET < Created <= CET or LSET < LastUpdated <= CET
        customer_df = customer_df[((customer_df['Created'] > LSET) & (customer_df['Created'] <= CET))
        | ((customer_df['LastUpdated'] > LSET) & (customer_df['LastUpdated'] <= CET))]
        
        # Insert new records into the staging table
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        errors_list = []
        with mssql_engine.connect() as connection:
            for index, row in customer_df.iterrows():
                try:
                    query = f"""
                        INSERT INTO S_customer 
                        VALUES ({row['CustomerID']}, '{row['Title']}', '{row['MiddleName']}', '{row['LastName']}', '{row['Country']}', '{row['EmailAddress']}', '{row['Phone']}', '{CET}', '{CET}')
                    """
                    connection.execute(query)
                except Exception as e:
                    print(f"An error occurred while inserting row : {e},", query)
                    err = {
                        'type': 'customer',
                        'query_string': str(query),
                        'Date': CET
                    }
                    errors_list.append(err)

            error_df = pd.DataFrame(errors_list)
            error_df.to_sql('error_log_general', ps_engine, if_exists='append', index=False) 
        update_LSET('S_customer', CET)
    @task
    def insert_product():
        CET = date.today()
        LSET = get_LSET('S_product')

        truncate_table('S_product')

        product_df = pd.read_csv('source_data/product.csv')
        product_df['Created'] = pd.to_datetime(product_df['Created']).dt.date
        product_df['LastUpdated'] = pd.to_datetime(product_df['LastUpdated']).dt.date
        print(CET)
        print(LSET)
        #Incremental update, LSET < Created <= CET or LSET < LastUpdated <= CET
        product_df = product_df[((product_df['Created'] > LSET) & (product_df['Created'] <= CET))
        | ((product_df['LastUpdated'] > LSET) & (product_df['LastUpdated'] <= CET))]
        
        # Insert new records into the staging table
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        errors_list = []
        with mssql_engine.connect() as connection:
            for index, row in product_df.iterrows():
                try:
                    query = f"""
                        INSERT INTO S_product
                        VALUES ({row['ProductID']}, '{row['PName']}', '{row['Color']}', {row['StandardCost']}, {row['ListPrice']}, '{row['Size']}', '{row['PWeight']}', '{CET}', '{CET}')
                    """
                    connection.execute(query)
                except Exception as e:  
                    print(f"An error occurred while inserting row : {e},", query)
                    err = {
                        'type': 'product',
                        'query_string': str(query),
                        'Date': CET
                    }
                    errors_list.append(err)

            error_df = pd.DataFrame(errors_list)
            error_df.to_sql('error_log_general', ps_engine, if_exists='append', index=False) 
        update_LSET('S_product', CET)

    @task
    def insert_sale():
        CET = date.today()
        LSET = get_LSET('S_sale')

        truncate_table('S_sale')

        sale_df = pd.read_csv('source_data/sale.csv')
        sale_df['Created'] = pd.to_datetime(sale_df['Created']).dt.date
        sale_df['LastUpdated'] = pd.to_datetime(sale_df['LastUpdated']).dt.date
        print(CET)
        print(LSET)
        #Incremental update, LSET < Created <= CET or LSET < LastUpdated <= CET
        sale_df = sale_df[((sale_df['Created'] > LSET) & (sale_df['Created'] <= CET))
        | ((sale_df['LastUpdated'] > LSET) & (sale_df['LastUpdated'] <= CET))]
        
        # Insert new records into the staging table
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        errors_list = []
        with mssql_engine.connect() as connection:
            for index, row in sale_df.iterrows():
                try:
                    query = f"""
                        INSERT INTO S_sale
                        VALUES ({row['SalesOrderID']}, '{row['ProductID']}', '{row['CustomerID']}', '{row['OrderQty']}', '{CET}', '{CET}')
                    """
                    connection.execute(query)
                except Exception as e:
                    print(f"An error occurred while inserting row: {e},", query)
                    err = {
                        'type': 'sale',
                        'query_string': str(query),
                        'Date': CET
                    }
                    errors_list.append(err)

            error_df = pd.DataFrame(errors_list)
            error_df.to_sql('error_log_general', ps_engine, if_exists='append', index=False)
        update_LSET('S_sale', CET)
    
    insert_country_currency() >> insert_customer() >> insert_product() >> insert_sale()
    #