from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import pymssql
import time

with DAG(
      dag_id="initialize",
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


        # Create SQLAlchemy engine with autocommit
        engine = create_engine(connection_string, isolation_level='AUTOCOMMIT')

        # Create a new database
        create_db_query = f'CREATE DATABASE [ap_airflow]'

        with engine.connect() as connection:
            try:
                connection.execute(create_db_query)
                print(f"Database [ap_airflow] created successfully.")
            except Exception as e:
                print(f"There is a problem creating the DB: ", e)
    @task
    def create_table_mssql():
        # Replace with your actual connection details
        server = 'host.docker.internal'
        database = 'ap_airflow'
        username = 'sa'
        password = '12345'
        time.sleep(2)

        # Create the connection string for pymssql
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)

        #country_currency START
        #The S_ prefix is to make it easier to insert metadata
        create_table_sql = """
        CREATE TABLE S_country_currency(
            ID int,
            EngName nvarchar(50),
            ISO char(5),
            ISO3 char(5),
            Dial nvarchar(10),
            Currency nvarchar(10),
            Created date,
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
        CREATE TABLE S_customer(
            CustomerID int,
            Title char(5),
            MiddleName nvarchar(20),
            LastName nvarchar(20),
            Country char(5),
            EmailAddress nvarchar(50),
            Phone nvarchar(50),
            Created date,
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
        CREATE TABLE S_product(
            ProductID int,
            PName nvarchar(100),
            Color char(15),
            StandardCost float,
            ListPrice float,
            Size char(10),
            PWeight float, 
            Created date,
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
        CREATE TABLE S_sale(
            SalesOrderID int,
            ProductID int,
            CustomerID int,
            OrderQty int,
            Created date,
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
        CREATE TABLE S_sale_detail(
            SalesOrderID int,
            ProductID int,
            CustomerID int,
            OrderQty int,
            SystemPrice float,
            LocalCurrency char(5),
            LocalPrice float,
            SalesCountry char(5),
            CreatedDate date,
            primary key(SalesOrderID, ProductID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table sale detail created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")
        #END

        #currency START
        create_table_sql = """
        CREATE TABLE S_currency(
            Code char(10),
            ConvertValue float,
            Created date,
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

        return "Initialization completed"
    
    @task 
    def create_metadata_mssql():
        # Replace with your actual connection details
        server = 'host.docker.internal'
        database = 'ap_airflow'
        username = 'sa'
        password = '12345'

        # Create the connection string for pymssql
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)

        create_table_sql = f"""
        CREATE TABLE metadata (
            ID INT IDENTITY,
            TABNAME VARCHAR(50),
            LSET DATETIME,

            CONSTRAINT PK_STAGE
            PRIMARY KEY(ID)
        )
        """
        try:
            with engine.connect() as connection:
                connection.execute(create_table_sql)
                print("Table metadata created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

        table_query = f"""
            SELECT TABLE_NAME, '2000-01-01 00:00:00.000' AS LSET
            FROM [ap_airflow].INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE'
            AND TABLE_NAME LIKE 'S_%'
        """
        table = (pd.read_sql(table_query, engine))
        print(table)
        with engine.connect() as connection:
            for index, row in table.iterrows():
                try:
                    insert_query = f"""
                        INSERT INTO metadata
                        VALUES
                        ('{row['TABLE_NAME']}','{row['LSET']}')
                    """
                    connection.execute(insert_query)
                except Exception as e:
                    print("Having error insert the metadata: ", e)

        return ("Populate metadata sucessfully")
    
    @task
    def initialize_postgres():
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
            "CreatedDate" DATE
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

        conn = ps_engine.connect()
        conn.execute("commit")
        result = conn.execute(f"""CREATE TABLE public.error_log_general(
            "type" CHAR(100),
            "query_string" VARCHAR,
            "Date" DATE
            );
        """)

        print(result)
        conn.close()


    create_mssql_database() >> create_table_mssql() >> create_metadata_mssql() >> initialize_postgres()

    #Try to ensure initialize complete before pouring source



  