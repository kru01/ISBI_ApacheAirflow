from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import os

with DAG(
      dag_id="initilize",
      start_date=datetime(2024, 1, 1, 9),
      schedule="@once",
      catchup=False,
      max_active_runs=1,
      default_args={
      "retries": 3,
      "retry_delay": timedelta(minutes=5)
      }
) as dag:
    @task 
    def create_mssql_database():
        server = 'host.docker.internal'
        database = 'master'
        username = 'sa'
        password = '12345'

        # Create the connection string for pymssql
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)
        check_query = f"""
            SELECT * FROM sys.databases WHERE name = 'ap_airflow'
        """
        with engine.connect() as connection:
            db = connection.execute(check_query).scalar_one_or_none()
            if db:
                return "ap_airflow DB already exists"
            else:
                create_query = f"""
                    CREATE DATABASE [ap_airflow]
                """
                result = connection.execute(text(create_query).execution_options(autocommit=True)).scalar_one_or_none()
                if result:
                    return "Create ap_airflow db successfully"
                else:
                    return "Something went wrong, can't create db"
    @task
    def initilize_mssql():
        # Replace with your actual connection details
        server = 'host.docker.internal'
        database = 'ap_airflow'
        username = 'sa'
        password = '12345'

        # Create the connection string for pymssql
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)

        #country_currency START
        create_table_sql = """
        CREATE TABLE country_currency(
            id int,
            eng_name nvarchar(50),
            iso char(5),
            iso3 char(5),
            dial nvarchar(10),
            currency nvarchar(10),
            LastUpdated date,
            primary key(id)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table country_currency created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END 

        #customer START
        create_table_sql = """
        CREATE TABLE customer(
            CustomerID int,
            Title char(5),
            MiddleName nvarchar(20),
            LastName nvarchar(20),
            Country char(5),
            EmailAddress nvarchar(50),
            Phone nvarchar(50),
            LastUpdated date,
            primary key(CustomerID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table customer created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END

        #product START
        create_table_sql = """
        CREATE TABLE product(
            ProductID int,
            PName nvarchar(100),
            Color char(15),
            StandardCost float,
            ListPrice float,
            Size char(10),
            PWeight float, 
            LastUpdated date,
            primary key(ProductID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table product created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END

        #sale START
        create_table_sql = """
        CREATE TABLE sale(
            SalesOrderID int,
            ProductID int,
            CustomerID int,
            OrderQty int,
            LastUpdated date,
            primary key(SalesOrderID, ProductID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table sale created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END

        #sale_detail START
        create_table_sql = """
        CREATE TABLE sale_detail(
            SalesOrderID int,
            ProductID int,
            CustomerID int,
            OrderQty int,
            SystemPrice float,
            LocalCurrency char(5),
            LocalPrice float,
            SalesCountry char(5),
            LastUpdated date,
            primary key(SalesOrderID, ProductID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table sale created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END

        #currency START
        create_table_sql = """
        CREATE TABLE currency(
            code char(10),
            conv_value float,
            LastUpdated date,
            primary key(code)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table currency created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END 

        return "Initilization completed"
    
    @task
    def initilize_postgres():
        #For database logging
        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/postgres")
        conn = ps_engine.connect()
        conn.execute("commit")
        #postgres does not allow create db inside a transaction, so we need to commit
        result = conn.execute("DROP DATABASE IF EXISTS logging")
        print(result)
        conn.close()

        conn = ps_engine.connect()
        conn.execute("commit")
        #postgres does not allow create db inside a transaction, so we need to commit
        result = conn.execute("CREATE DATABASE logging")
        print(result)
        conn.close()

        ps_engine = create_engine("postgresql://postgres:postgres@host.docker.internal/logging")
        #For table sale_detail_error_logs
        conn = ps_engine.connect()
        conn.execute("commit")
        result = conn.execute(f"""CREATE TABLE public.sale_detail_error_logs(
            row_id SERIAL PRIMARY KEY,
            "SalesOrderID" INT,
            "ProductID" INT,
            "CustomerID" INT,
            "OrderQty" INT,
            "SystemPrice" FLOAT,
            "LocalCurrency" CHAR(5),
            "LocalPrice" FLOAT,
            "SalesCountry" CHAR(5),
            "Error" VARCHAR,
            "LastUpdated" DATE
            );
        """)
        print(result)
        conn.close()

        #For currency_error_log
        conn = ps_engine.connect()
        conn.execute("commit")
        result = conn.execute(f"""CREATE TABLE public.currency_error_logs(
            row_id SERIAL PRIMARY KEY,
            "code" CHAR(100),
            "conv_value" FLOAT,
            "error" VARCHAR,
            "LastUpdated" DATE
            );
        """)

        print(result)
        conn.close()


    flag_1 = create_mssql_database()
    print(flag_1)
    flag_2 = initilize_mssql()
    print(flag_2)
    initilize_postgres()

    #Try to ensure initilize complete before pouring source



  