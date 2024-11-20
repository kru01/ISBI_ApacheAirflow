from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import pymssql
import time

server = 'host.docker.internal'
database = 'ap_airflow'
username = 'sa'
password = '12345'

# Create the connection string for pymssql
connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
mssql_engine = create_engine(connection_string)


with DAG(
      dag_id="initialize_metadata",
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
    def create_ETL_package():
        query = f"""
            CREATE TABLE ETL_PACKAGE (
            ID INT IDENTITY PRIMARY KEY,
            [NAME] VARCHAR(150)
            )
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)
        query = f"""
            INSERT INTO ETL_PACKAGE
            VALUES ('SRC_STAGE'), ('STAGE_NDS'), ('NDS_DDS')
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)
    
    @task 
    def create_ETL_status():
        query = f"""
            CREATE TABLE ETL_STATUS (
            ID INT IDENTITY PRIMARY KEY,
            [NAME] VARCHAR(50)
            )
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)
        query = f"""
            INSERT INTO ETL_STATUS
            VALUES ('UNKNOWN'), ('SUCCESS'),
                ('FAILED'), ('IN PROGRESS')
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)
    
    @task 
    def create_ETL_dataflow():
        query = f"""
            CREATE TABLE ETL_DATAFLOW (
            ID INT IDENTITY,
            [NAME] VARCHAR(150),
            [DESC] VARCHAR(250),
            SRC VARCHAR(250),
            [TARGET] VARCHAR(250),
            TRANSF VARCHAR(250),
            PACKAGE INT,
            [STATUS] INT,
            LSET DATETIME,

            CONSTRAINT PK_ETL_DATAFLOW
            PRIMARY KEY(ID),

            CONSTRAINT FK_ETL_DATAFLOW_ETL_PACKAGE
            FOREIGN KEY(PACKAGE) REFERENCES ETL_PACKAGE(ID),

            CONSTRAINT FK_ETL_DATAFLOW_ETL_STATUS
            FOREIGN KEY([STATUS]) REFERENCES ETL_STATUS(ID)
            )
        """
        with mssql_engine.connect() as connection:
                connection.execute(query)

        query = f"""
        INSERT INTO ETL_DATAFLOW
        VALUES
            ('STAGE_CURRENCY', '', 'API.CURRENCY', 'STAGE.CURRENCY', '', 1, 1, '2000/01/01 00:00:00'),
            ('STAGE_COUNTRY_CURRENCY', '', 'CSV.COUNTRY_CURRENCY', 'STAGE_COUNTRY_CURRENCY', '', 1, 1, '2000/01/01 00:00:00'),
            ('STAGE_CUSTOMER', '', 'CSV.CUSTOMER', 'STAGE.CUSTOMER', '', 1, 1, '2000/01/01 00:00:00'),
            ('STAGE_PRODUCT', '', 'CSV.PRODUCT', 'STAGE.PRODUCT', '', 1, 1, '2000/01/01 00:00:00'),
            ('STAGE_SALE', '', 'CSV.SALE', 'STAGE.SALE', '', 1, 1, '2000/01/01 00:00:00'),
            ('NDS_SALE_DETAIL', '', 'STAGE.SALE', 'NDS_SALE_DETAIL', '', 1, 1, '2000/01/01 00:00:00')
        """
        with mssql_engine.connect() as connection:
                connection.execute(query)

    @task 
    def create_DQ_rtype():
        query_dq_rtype_create = f"""
            CREATE TABLE DQ_RTYPE (
            ID VARCHAR(5) PRIMARY KEY,
            [NAME] VARCHAR(50),
            )
        """

        query_dq_rtype_insert = f"""
            INSERT INTO DQ_RTYPE
            VALUES ('E', 'ERROR'), ('W', 'WARNING')
        """
        # Create and populate DQ_RCATE table
        query_dq_rcate_create = """
            CREATE TABLE DQ_RCATE (
                ID VARCHAR(5) PRIMARY KEY,
                [NAME] VARCHAR(50)
            )
        """
        query_dq_rcate_insert = """
            INSERT INTO DQ_RCATE (ID, [NAME])
            VALUES 
                ('I', 'INCOMING DATA'),
                ('C', 'CROSS-REFERENCE'),
                ('D', 'INTERNAL DW')
        """

        # Create and populate DQ_RRISK table
        query_dq_rrisk_create = """
            CREATE TABLE DQ_RRISK (
                ID INT IDENTITY PRIMARY KEY,
                [NAME] VARCHAR(50)
            )
        """
        query_dq_rrisk_insert = """
            INSERT INTO DQ_RRISK ([NAME])
            VALUES 
                ('NONE'),
                ('LOW'),
                ('MILD'),
                ('SEVERE'),
                ('CRITICAL')
        """

        # Create and populate DQ_RSTATUS table
        query_dq_rstatus_create = """
            CREATE TABLE DQ_RSTATUS (
                ID VARCHAR(5) PRIMARY KEY,
                [NAME] VARCHAR(50)
            )
        """
        query_dq_rstatus_insert = """
            INSERT INTO DQ_RSTATUS (ID, [NAME])
            VALUES 
                ('A', 'ACTIVE'),
                ('D', 'DECOMMISSIONED')
        """

        # Create and populate DQ_RACTION table
        query_dq_raction_create = """
            CREATE TABLE DQ_RACTION (
                ID VARCHAR(5) PRIMARY KEY,
                [NAME] VARCHAR(50)
            )
        """
        query_dq_raction_insert = """
            INSERT INTO DQ_RACTION (ID, [NAME])
            VALUES 
                ('R', 'REJECT'),
                ('A', 'ALLOW'),
                ('F', 'FIX')
        """

        # Execute all queries
        with mssql_engine.connect() as connection:
            connection.execute(query_dq_rtype_create)
            connection.execute(query_dq_rtype_insert)

            connection.execute(query_dq_rcate_create)
            connection.execute(query_dq_rcate_insert)
            
            connection.execute(query_dq_rrisk_create)
            connection.execute(query_dq_rrisk_insert)
            
            connection.execute(query_dq_rstatus_create)
            connection.execute(query_dq_rstatus_insert)
            
            connection.execute(query_dq_raction_create)
            connection.execute(query_dq_raction_insert)

    @task
    def create_DQ_rule():
        query = f"""
            CREATE TABLE DQ_RULE (
            ID INT IDENTITY,
            [NAME] VARCHAR(150),
            [DESC] VARCHAR(250),
            [TYPE] VARCHAR(5),
            CATE VARCHAR(5),
            RISK INT,
            [STATUS] VARCHAR(5),
            [ACTION] VARCHAR(5),
            CREATED DATETIME,
            UPDATED DATETIME,

            CONSTRAINT PK_DQ_RULE
            PRIMARY KEY(ID),

            CONSTRAINT FK_DQ_RULE_DQ_RTYPE
            FOREIGN KEY([TYPE]) REFERENCES DQ_RTYPE(ID),

            CONSTRAINT FK_DQ_RULE_DQ_RCATE
            FOREIGN KEY(CATE) REFERENCES DQ_RCATE(ID),

            CONSTRAINT FK_DQ_RULE_DQ_RRISK
            FOREIGN KEY(RISK) REFERENCES DQ_RRISK(ID),

            CONSTRAINT FK_DQ_RULE_DQ_RSTATUS
            FOREIGN KEY([STATUS]) REFERENCES DQ_RSTATUS(ID),

            CONSTRAINT FK_DQ_RULE_DQ_RACTION
            FOREIGN KEY([ACTION]) REFERENCES DQ_RACTION(ID)
            )
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)

        query = f"""
            INSERT INTO DQ_RULE
            VALUES
            ('COUNTRY_CODE', 'WE HAVE NOT OPERATE IN THIS COUNTRY YET',
                'E', 'I', 4, 'A', 'R', GETDATE(), GETDATE()),
            ('REFERENCE_ERROR', 'REFERENCE TO A NON EXISTS DATA',
                'E', 'C', 4, 'A', 'R', GETDATE(), GETDATE())
            """
        with mssql_engine.connect() as connection:
            connection.execute(query)
    
    @task
    def create_DQ_rule_fail():
        query = f"""
            CREATE TABLE DQ_FAILURE (
            ID INT IDENTITY PRIMARY KEY,
            FLOW_ID INT,
            TAB NVARCHAR(250),
            [KEY] nvarchar(10),
            [RULE] INT,
            CREATED DATETIME,
            
            CONSTRAINT FK_DQ_FAILURE_DQ_RULE
            FOREIGN KEY([RULE]) REFERENCES DQ_RULE(ID),

            CONSTRAINT FK_DQ_FAILURE_ETL_DATAFLOW
            FOREIGN KEY(FLOW_ID) REFERENCES ETL_DATAFLOW(ID)
            )
        """
        with mssql_engine.connect() as connection:
            connection.execute(query)


    create_ETL_package() >> create_ETL_status() >> create_ETL_dataflow() >> create_DQ_rtype() >> create_DQ_rule() >> create_DQ_rule_fail()