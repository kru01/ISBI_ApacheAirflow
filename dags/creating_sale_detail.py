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
engine = create_engine(connection_string)

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
     
    @task
    def createing_sale_dt(engine):
        #Connect postgres for error logs
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        error_list = []

        #As the script proposed, we are currently operating on 3 country USA, CAD and AUS. Rows with other country will be considered invalid.
        operated_country = ['USA', 'CAN', 'AUS']

        # Query to fetch data
        query = "SELECT * FROM S_sale"
        new_sale_record = pd.read_sql(query, engine)

        CET = date.today()
        LSET = get_LSET('S_sale_detail')

        new_sale_record = new_sale_record[((new_sale_record['Created'] > LSET) & (new_sale_record['Created'] <= CET))
        | ((new_sale_record['LastUpdated'] > LSET) & (new_sale_record['LastUpdated'] <= CET))]

        #Current system rate is USD
        system_iso3 = 'USA'
        system_currency = 'USD'
        system_currency_rate_query = f"SELECT ConvertValue FROM S_currency WHERE code='{system_currency}'"
        system_currency_rate = (pd.read_sql(system_currency_rate_query, engine))['ConvertValue'][0]
        print("The rate for system currency is: ", system_currency_rate )

        
        for index, row in new_sale_record.iterrows():
            try: 
                product_query = f"SELECT ListPrice FROM S_product WHERE ProductID = '{row['ProductID']}'"
                product_price = (pd.read_sql(product_query, engine))['ListPrice'][0]
                system_price = product_price * row['OrderQty']
                # print("system_price: ", system_price)

                customer_country_query = f"SELECT Country FROM S_customer WHERE CustomerID = '{row['CustomerID']}'"
                customer_country = (pd.read_sql(customer_country_query, engine))['Country'][0]
                customer_country = customer_country.strip()
                print("customer_country: ", customer_country)

                customer_country_currency_query = f"SELECT Currency FROM S_country_currency WHERE ISO3='{customer_country}'"
                customer_country_currency= (pd.read_sql(customer_country_currency_query, engine))['Currency'][0]
                # print("customer_country_currency: ", customer_country_currency)

                local_rate_query = f"SELECT ConvertValue FROM S_currency WHERE code='{customer_country_currency}'"
                local_rate = (pd.read_sql(local_rate_query, engine))['ConvertValue'][0]
                # print("local_rate: ", local_rate)

                local_price = (system_price/ system_currency_rate) * local_rate
                # print("This is the result")
                
                #query to insert new row into sale_detail
                query = f"""
                     INSERT INTO S_sale_detail
                    VALUES ({row['SalesOrderID']}, {row['ProductID']}, {row['CustomerID']}, {row['OrderQty']}, {system_price}, '{customer_country_currency}', {local_price}, '{customer_country}', '{CET}')
                """
                print(query)
                if customer_country not in operated_country:
                    err = {
                                'SalesOrderID' : row['SalesOrderID'],
                                'ProductID' : row['ProductID'],
                                'CustomerID' :row['CustomerID'],
                                'OrderQty' :row['OrderQty'],
                                'SystemPrice' : system_price,
                                'LocalCurrency' : customer_country_currency,
                                'LocalPrice' : local_price,
                                'SalesCountry' : customer_country,
                                'Error': f"We are not operating on this country yet: {customer_country}",
                                'CreatedDate': CET
                    }
                    error_list.append(err)
                else:
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
                                'CreatedDate': CET
                            }
                        error_list.append(err)
            except Exception as e:
                print("Having error extracting information", index, " | ", e)
        error_df = pd.DataFrame(error_list)
        error_df.to_sql('sale_detail_error_logs', ps_engine, if_exists='append', index=False) 
        update_LSET('S_sale_detail', CET)
   
    createing_sale_dt(engine)
  