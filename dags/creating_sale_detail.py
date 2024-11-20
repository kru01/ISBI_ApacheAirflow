from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
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
            SELECT LSET FROM ETL_DATAFLOW
            WHERE [NAME] = '{table_name}'
        """
        LSET = pd.read_sql(query, engine)
        return (LSET['LSET'][0]).date()
    def update_LSET(table_name, LSET):
        query = f"""
            UPDATE ETL_DATAFLOW
            SET LSET = '{LSET}'
            WHERE [NAME] = '{table_name}'
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
        query = "SELECT * FROM STAGE_sale"
        new_sale_record = pd.read_sql(query, engine)

        CET = date.today()
        LSET = get_LSET('NDS_sale_detail')

        new_sale_record = new_sale_record[((new_sale_record['Created'] > LSET) & (new_sale_record['Created'] <= CET))
        | ((new_sale_record['LastUpdated'] > LSET) & (new_sale_record['LastUpdated'] <= CET))]

        #Current system rate is USD
        system_iso3 = 'USA'
        system_currency = 'USD'
        system_currency_rate_query = f"SELECT ConvertValue FROM STAGE_currency WHERE code='{system_currency}'"
        system_currency_rate = (pd.read_sql(system_currency_rate_query, engine))['ConvertValue'][0]
        print("The rate for system currency is: ", system_currency_rate )

        
        for index, row in new_sale_record.iterrows():
            try:   
                try:
                    product_query = f"SELECT ListPrice FROM STAGE_product WHERE ProductID = '{row['ProductID']}'"
                    product_price = (pd.read_sql(product_query, engine))['ListPrice'][0]
                    print("Test product price", product_price)
                except KeyError as e:
                    print(f"KeyError: Missing key in 'row': {e}")
                    raise Exception("product is missing from the input data")
                except IndexError as e:
                    print(f"IndexError: Query returned no results: {e}")
                    raise Exception("product is missing in the database")
                except SQLAlchemyError as e:
                    print(f"SQLAlchemy error occurred: {e}")
                    raise Exception("product reference error")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    raise
    
                system_price = product_price * row['OrderQty']
                print("system_price: ", system_price)

                try:
                    customer_country_query = f"SELECT Country FROM STAGE_customer WHERE CustomerID = '{row['CustomerID']}'"
                    customer_country = (pd.read_sql(customer_country_query, engine))['Country'][0]
                    customer_country = customer_country.strip()
                    print("customer_country: ", customer_country)
                except KeyError as e:
                    print(f"KeyError: Missing key in 'row': {e}")
                    raise Exception("customer is missing from the input data")
                except IndexError as e:
                    print(f"IndexError: Query returned no results: {e}")
                    raise Exception("customer is missing in the database")
                except SQLAlchemyError as e:
                    print(f"SQLAlchemy error occurred: {e}")
                    raise Exception("customer reference error")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    raise

                try:
                    customer_country_currency_query = f"SELECT Currency FROM STAGE_country_currency WHERE ISO3='{customer_country}'"
                    customer_country_currency= (pd.read_sql(customer_country_currency_query, engine))['Currency'][0]
                    print("customer_country_currency: ", customer_country_currency)
                except KeyError as e:
                    print(f"KeyError: Missing key in 'row': {e}")
                    raise Exception("country_currency is missing from the input data")
                except IndexError as e:
                    print(f"IndexError: Query returned no results: {e}")
                    raise Exception("country_currency is missing in the database")
                except SQLAlchemyError as e:
                    print(f"SQLAlchemy error occurred: {e}")
                    raise Exception("country_currency reference error")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    raise

                try:
                    local_rate_query = f"SELECT ConvertValue FROM STAGE_currency WHERE code='{customer_country_currency}'"
                    local_rate = (pd.read_sql(local_rate_query, engine))['ConvertValue'][0]
                    print("local_rate: ", local_rate)
                except KeyError as e:
                    print(f"KeyError: Missing key in 'row': {e}")
                    raise Exception("currency is missing from the input data")
                except IndexError as e:
                    print(f"IndexError: Query returned no results: {e}")
                    raise Exception("currency is missing in the database")
                except SQLAlchemyError as e:
                    print(f"SQLAlchemy error occurred: {e}")
                    raise Exception("currency reference error")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    raise
                local_price = (system_price/ system_currency_rate) * local_rate
                print("This is the result")
                
                #query to insert new row into sale_detail
                query = f"""
                     INSERT INTO NDS_sale_detail
                    VALUES ({row['SalesOrderID']}, {row['ProductID']}, {row['CustomerID']}, {row['OrderQty']}, {system_price}, '{customer_country_currency}', {local_price}, '{customer_country}', '{CET}')
                """
                print(query)
                if customer_country not in operated_country:
                    query = f"""
                        INSERT INTO DQ_FAILURE
                        VALUES
                        (6, 'STAGE_sale', '{row['SalesOrderID']}', 1, '{CET}')
                    """
                    with engine.connect() as connection:
                            connection.execute(query)
                else:
                    try:
                        with engine.connect() as connection:
                            connection.execute(query)
                    except Exception as e:
                        print("Having error insert a new sale_detail row: ", e)
            except Exception as e:
                print("Having error extracting information", index, " | ", str(e))
                error_message = str(e)
                if ("missing" in error_message):
                    TABNAME = error_message.split(' ')[0]
                    key = row['ProductID']
                    if (TABNAME == 'customer'): key = row['CustomerID']
                    if (TABNAME == 'country_currency'): key = customer_country
                    if (TABNAME == 'currency'): key = customer_country_currency
                    query = f"""
                        INSERT INTO DQ_FAILURE
                        VALUES
                        (6, 'STAGE_{TABNAME}', '{key}', 2, '{CET}')
                    """ 
                    with engine.connect() as connection:
                            connection.execute(query)
        update_LSET('NDS_sale_detail', CET)
   
    createing_sale_dt(engine)
  