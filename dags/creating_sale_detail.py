from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
import pandas as pd

with DAG(
      dag_id="creating_sale_detail",
      start_date=datetime(2024, 1, 1, 9),
      schedule='0 12 * * *',
      catchup=False,
      max_active_runs=1,
      default_args={
      "retries": 3,
      "retry_delay": timedelta(minutes=5)
      }
) as dag:
    @task
    def finding_price(engine):
        #Connect postgres for error logs
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        error_list = []

        # Query to fetch data
        query = "SELECT * FROM sale"
        sale_df = pd.read_sql(query, engine)
        with engine.connect() as connection:
            latest_timestamp_query = "SELECT COALESCE(MAX(LastUpdated), '1900-01-01') FROM sale_detail"
            latest_timestamp = connection.execute(latest_timestamp_query).scalar()
        new_sale_records = sale_df[sale_df['LastUpdated'] > latest_timestamp]

        #Current system rate is USD
        system_iso3 = 'USA'
        system_currency = 'USD'
        system_curerncy_rate_query = f"SELECT conv_value FROM currency WHERE code='{system_currency}'"
        system_curerncy_rate = (pd.read_sql(system_curerncy_rate_query, engine))['conv_value'][0]
        print("The rate for system currency is: ", system_curerncy_rate )

        
        for index, row in new_sale_records.iterrows():
            # print(f"Index: {index}")
            # print(row)
            # Access individual elements with row['column_name']
            try: 
                product_query = f"SELECT ListPrice FROM product WHERE ProductID = '{row['ProductID']}'"
                product_price = (pd.read_sql(product_query, engine))['ListPrice'][0]
                system_price = product_price * row['OrderQty']
                # print("system_price: ", system_price)

                customer_country_query = f"SELECT Country FROM customer WHERE CustomerID = '{row['CustomerID']}'"
                customer_country = (pd.read_sql(customer_country_query, engine))['Country'][0]
                customer_country = customer_country.strip()
                # print("customer_country: ", customer_country)

                customer_country_currency_query = f"SELECT currency FROM country_currency WHERE iso3='{customer_country}'"
                customer_country_currency= (pd.read_sql(customer_country_currency_query, engine))['currency'][0]
                # print("customer_country_currency: ", customer_country_currency)

                local_rate_query = f"SELECT conv_value FROM currency WHERE code='{customer_country_currency}'"
                local_rate = (pd.read_sql(local_rate_query, engine))['conv_value'][0]
                # print("local_rate: ", local_rate)

                local_price = (system_price/ system_curerncy_rate) * local_rate
                # print("This is the result")
                
                #query to insert new row into sale_detail
                query = f"""
                     INSERT INTO sale_detail
                    VALUES ({row['SalesOrderID']}, {row['ProductID']}, {row['CustomerID']}, {row['OrderQty']}, {system_price}, '{customer_country_currency}', {local_price}, '{customer_country}', '{row['LastUpdated']}')
                """
                try:
                    with engine.connect() as connection:
                        connection.execute(query)
                except Exception as e:
                    print("Having error insert a new sale_detail row: ", e)
                    err = {
                            'SalesOrderID' : row['SalesOrderID'],
                            'ProductID' : row['ProductID'],
                            'CustomerID' :row['CustomerID'],
                            'OrderQty' :row['OrderQty'],
                            'SystemPrice' : system_price,
                            'LocalCurrency' : customer_country_currency,
                            'LocalPrice' : local_price,
                            'SalesCountry' : customer_country,
                            'Error': str(e),
                            'LastUpdated': row['LastUpdated']
                        }
                    error_list.append(err)
            except:
                print("Having error extracting information", index)
        error_df = pd.DataFrame(error_list)
        error_df.to_sql('sale_detail_error_logs', ps_engine, if_exists='append', index=False) 

    server = 'host.docker.internal'
    database = 'ap_airflow'
    username = 'sa'
    password = '12345'

    # Create the connection string for pymssql
    connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
    engine = create_engine(connection_string)   
    finding_price(engine)
  