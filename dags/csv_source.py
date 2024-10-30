from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pandas as pd

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
    def insert_country_currency(engine):
        today = date.today()
        country_currency_df = pd.read_csv('source_data/country_currency.csv')
        country_currency_df['LastUpdated'] = pd.to_datetime(country_currency_df['LastUpdated']).dt.date
        with engine.connect() as connection:
            latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM country_currency"
            latest_timestamp = connection.execute(latest_timestamp_query).scalar()

        # Filter the DataFrame for new records
        new_records = country_currency_df[country_currency_df['LastUpdated'] > latest_timestamp]

        # Insert new records into the staging table
        if not new_records.empty:
            ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
            errors_list = []
            with engine.connect() as connection:
                for index, row in new_records.iterrows():
                    try:
                        check_query = f"""
                            SELECT * FROM country_currency WHERE id='{row['id']}'
                        """
                        current_c_currency = connection.execute(check_query).scalar_one_or_none()
                        print(current_c_currency)
                        if current_c_currency:
                            update_query = f"""
                                UPDATE country_currency
                                SET eng_name={row['eng_name']}, iso={row['iso']}, iso3={row['iso3']}, dial={row['dial']}, currency={row['currency']}, LastUpdated='{row['LastUpdated']}'
                                WHERE code='{row['code']}'
                            """
                            connection.execute(update_query)
                        else:
                            insert_query = f"""
                                INSERT INTO country_currency 
                                VALUES ({row['id']}, '{row['eng_name']}', '{row['iso']}', '{row['iso3']}', '{row['dial']}', '{row['currency']}', '{row['LastUpdated']}')
                            """
                        connection.execute(insert_query)
                    except Exception as e:
                        print(f"An error occurred while inserting row {row['id']}: {e}")
                        err = {
                            'type': 'country_currency',
                            'row_id': row['id'],
                            'Date': today
                        }
                        errors_list.append(err)

            # error_df = pd.DataFrame(errors_list)
            # error_df.to_sql('error_logs', ps_engine, if_exists='append', index=False) 

    def insert_customer(engine):
        today = date.today()
        customer_df = pd.read_csv('source_data/customer.csv')
        customer_df['LastUpdated'] = pd.to_datetime(customer_df['LastUpdated']).dt.date
        with engine.connect() as connection:
            latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM customer"
            latest_timestamp = connection.execute(latest_timestamp_query).scalar()

        # Filter the DataFrame for new records
        new_records = customer_df[customer_df['LastUpdated'] > latest_timestamp]
        # Insert new records into the staging table
        if not new_records.empty:
            ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
            errors_list = []

            with engine.connect() as connection:
                for index, row in new_records.iterrows():
                    # insert_query = f"""
                    # INSERT INTO customer
                    # VALUES ({row['CustomerID']}, '{row['Title']}', '{row['MiddleName']}', '{row['LastName']}', '{row['Country']}', '{row['EmailAddress']}', '{row['Phone']}', '{row['LastUpdated']}')
                    # """
                    try:
                        check_query = f"""
                            SELECT * FROM customer WHERE CustomerID='{row['CustomerID']}'
                        """
                        current_customer = connection.execute(check_query).scalar_one_or_none()
                        if current_customer:
                            update_query = f"""
                                UPDATE customer
                                SET Title={row['Title']}, MiddleName={row['MiddleName']}, LastName={row['LastName']}, Country={row['Country']}, EmailAddress={row['EmailAddress']}, Phone={row['Phone']}, LastUpdated='{row['LastUpdated']}'
                                WHERE CustomerID='{row['CustomerID']}'
                            """
                            connection.execute(update_query)
                        else:
                            insert_query = f"""
                                INSERT INTO customer 
                                VALUES ({row['CustomerID']}, '{row['Title']}', '{row['MiddleName']}', '{row['LastName']}', '{row['Country']}', '{row['EmailAddress']}', '{row['Phone']}', '{row['LastUpdated']}')
                            """
                        connection.execute(insert_query)
                    except Exception as e:
                        print(f"An error occurred while inserting row {row['row_id']}: {e}")
                        err = {
                            'type': 'customer',
                            'row_id': row['row_id'],
                            'Date': today
                        }
                        errors_list.append(err)

            # error_df = pd.DataFrame(errors_list)
            # error_df.to_sql('error_logs', ps_engine, if_exists='append', index=False) 

    def insert_product(engine):
        today = date.today()
        product_df = pd.read_csv('source_data/product.csv')
        product_df['LastUpdated'] = pd.to_datetime(product_df['LastUpdated']).dt.date
        product_df.fillna('-1', inplace=True)
        with engine.connect() as connection:
            latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM product"
            latest_timestamp = connection.execute(latest_timestamp_query).scalar()

        # Filter the DataFrame for new records
        new_records = product_df[product_df['LastUpdated'] > latest_timestamp]
        # Insert new records into the staging table
        if not new_records.empty:
            ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
            errors_list = []
            with engine.connect() as connection:
                for index, row in new_records.iterrows():
                    try:
                        check_query = f"""
                            SELECT * FROM product WHERE ProductID='{row['ProductID']}'
                        """
                        current_product = connection.execute(check_query).scalar_one_or_none()
                        if current_product:
                            update_query = f"""
                                UPDATE product
                                SET PName={row['PName']}, Color={row['Color']}, StandardCost={row['StandardCost']}, ListPrice={row['ListPrice']}, Size={row['Size']}, PWeight={row['PWeight']}, LastUpdated='{row['LastUpdated']}'
                                WHERE ProductID='{row['ProductID']}'
                            """
                            connection.execute(update_query)
                        else:
                            insert_query = f"""
                                INSERT INTO product
                                VALUES ({row['ProductID']}, '{row['PName']}', '{row['Color']}', '{row['StandardCost']}', '{row['ListPrice']}', '{row['Size']}', '{row['PWeight']}', '{row['LastUpdated']}')
                            """
                        connection.execute(insert_query)
                    except Exception as e:
                        print(f"An error occurred while inserting row {row['row_id']}: {e}")
                        err = {
                            'type': 'product',
                            'row_id': row['row_id'],
                            'Date': today
                        }
                        errors_list.append(err)

            # error_df = pd.DataFrame(errors_list)
            # error_df.to_sql('error_logs', ps_engine, if_exists='append', index=False) 

    def insert_sale(engine):
        today = date.today()
        sale_df = pd.read_csv('source_data/sale.csv')
        sale_df['LastUpdated'] = pd.to_datetime(sale_df['LastUpdated']).dt.date
        with engine.connect() as connection:
            latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM sale"
            latest_timestamp = connection.execute(latest_timestamp_query).scalar()
        new_records = sale_df[sale_df['LastUpdated'] > latest_timestamp]
        # Insert new records into the staging table
        if not new_records.empty:
            ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
            errors_list = []
            with engine.connect() as connection:
                for index, row in new_records.iterrows():
                    #With order table we will not changing the content of its, if needed we only delete the order.
                    insert_query = f"""
                    INSERT INTO sale
                    VALUES ({row['SalesOrderID']}, '{row['ProductID']}', '{row['CustomerID']}', '{row['OrderQty']}', '{row['LastUpdated']}')
                    """
                    try:
                        connection.execute(insert_query)
                    except Exception as e:
                        print(f"An error occurred while inserting row {row['row_id']}: {e}")
                        err = {
                            'type': 'product',
                            'row_id': row['row_id'],
                            'Date': today
                        }
                        errors_list.append(err)

            # error_df = pd.DataFrame(errors_list)
            # error_df.to_sql('error_logs', ps_engine, if_exists='append', index=False) 


    @task
    def pouring_source():
        server = 'host.docker.internal'
        database = 'ap_airflow'
        username = 'sa'
        password = '12345'

        # Create the connection string for pymssql
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)
        
        #country_currency
        try:
            insert_country_currency(engine)
            print("Pouring country currency source")
        except Exception as e:
            print(f"An error occurred while pouring data from country currency source: {e}")

        #customer
        try:
            insert_customer(engine)
            print("Pouring customer source")
        except Exception as e:
            print(f"An error occurred while pouring data from customer source: {e}")

        #product
        try:
            insert_product(engine)
            print("Pouring product source")
        except Exception as e:
            print(f"An error occurred while pouring data from product source: {e}")

        #sale
        try:
            insert_sale(engine)
            print("Pouring sale source")
        except Exception as e:
            print(f"An error occurred while pouring data from sale source: {e}")

    pouring_source()
    #Try to ensure initilize complete before pouring source



  