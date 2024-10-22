from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
import pandas as pd
import requests

with DAG(
        dag_id="currency_source_to_stage",
        start_date=datetime(2024, 1, 1, 9),
        schedule="@monthly",
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
            token = "cur_live_NB9zFqtbV6mVtum8uOlS1bZsg7cVMdqBwacqXQZW@"
            #Remove the '@' at the end of the token in real run
            #WARNING!!! There is limit for api hit !!!WARNING
            deployment_url = f"https://api.currencyapi.com/v3/latest?apikey={token}"
            print(deployment_url)
            # response = requests.get(
            #     url=deployment_url
            # )
            # return response.json() 
            return {
                "meta": {
                    "last_updated_at": "2024-10-21T23:59:59Z"
                },
                "data": {
                    "ADA": {
                    "code": "ADA",
                    "value": 2.7654694416
                    },
                    "AED": {
                    "code": "AED",
                    "value": 3.6726003905
                    },
                    "AFN": {
                    "code": "AFN",
                    "value": 66.3516794303
                    },
                    "ALL": {
                    "code": "ALL",
                    "value": 91.1010777923
                    },
                    "AMD": {
                    "code": "AMD",
                    "value": 388.276037298
                    },
                    "ANG": {
                    "code": "ANG",
                    "value": 1.7863302162
                    },
                    "AOA": {
                    "code": "AOA",
                    "value": 912.8134523293
                    },
                    "ARB": {
                    "code": "ARB",
                    "value": 1.696609606
                    },
                    "ARS": {
                    "code": "ARS",
                    "value": 982.8201269875
                    },
                    "AUD": {
                    "code": "AUD",
                    "value": 1.5035002099
                    },
                    "AVAX": {
                    "code": "AVAX",
                    "value": 0.0359475414
                    },
                    "AWG": {
                    "code": "AWG",
                    "value": 1.79
                    },
                    "AZN": {
                    "code": "AZN",
                    "value": 1.7
                    },
                    "BAM": {
                    "code": "BAM",
                    "value": 1.8080902628
                    },
                    "BBD": {
                    "code": "BBD",
                    "value": 2
                    },
                    "BDT": {
                    "code": "BDT",
                    "value": 119.9927766298
                    },
                    "BGN": {
                    "code": "BGN",
                    "value": 1.8011702918
                    },
                    "BHD": {
                    "code": "BHD",
                    "value": 0.376
                    },
                    "BIF": {
                    "code": "BIF",
                    "value": 2895.271594879
                    },
                    "BMD": {
                    "code": "BMD",
                    "value": 1
                    },
                    "BNB": {
                    "code": "BNB",
                    "value": 0.0016558116
                    },
                    "BND": {
                    "code": "BND",
                    "value": 1.3160601894
                    },
                    "BOB": {
                    "code": "BOB",
                    "value": 6.9127107702
                    },
                    "BRL": {
                    "code": "BRL",
                    "value": 5.7025510585
                    },
                    "BSD": {
                    "code": "BSD",
                    "value": 1
                    },
                    "BTC": {
                    "code": "BTC",
                    "value": 0.0000148125
                    },
                    "BTN": {
                    "code": "BTN",
                    "value": 84.1890100578
                    },
                    "BWP": {
                    "code": "BWP",
                    "value": 13.3557314758
                    },
                    "BYN": {
                    "code": "BYN",
                    "value": 3.2699369614
                    },
                    "BYR": {
                    "code": "BYR",
                    "value": 32699.360428817
                    },
                    "BZD": {
                    "code": "BZD",
                    "value": 2
                    },
                    "CAD": {
                    "code": "CAD",
                    "value": 1.383660156
                    },
                    "CDF": {
                    "code": "CDF",
                    "value": 2839.8718449349
                    },
                    "CHF": {
                    "code": "CHF",
                    "value": 0.8657301484
                    },
                    "CLF": {
                    "code": "CLF",
                    "value": 0.0247300038
                    },
                    "CLP": {
                    "code": "CLP",
                    "value": 952.4199717885
                    },
                    "CNY": {
                    "code": "CNY",
                    "value": 7.1171608162
                    },
                    "COP": {
                    "code": "COP",
                    "value": 4268.0318647402
                    },
                    "CRC": {
                    "code": "CRC",
                    "value": 513.5781975477
                    },
                    "CUC": {
                    "code": "CUC",
                    "value": 1
                    },
                    "CUP": {
                    "code": "CUP",
                    "value": 24
                    },
                    "CVE": {
                    "code": "CVE",
                    "value": 101.9714729759
                    },
                    "CZK": {
                    "code": "CZK",
                    "value": 23.3591237842
                    },
                    "DAI": {
                    "code": "DAI",
                    "value": 0.9979010176
                    },
                    "DJF": {
                    "code": "DJF",
                    "value": 177.721
                    },
                    "DKK": {
                    "code": "DKK",
                    "value": 6.8953408183
                    },
                    "DOP": {
                    "code": "DOP",
                    "value": 60.1514680398
                    },
                    "DOT": {
                    "code": "DOT",
                    "value": 0.2279707377
                    },
                    "DZD": {
                    "code": "DZD",
                    "value": 133.9925597753
                    },
                    "EGP": {
                    "code": "EGP",
                    "value": 48.6598090479
                    },
                    "ERN": {
                    "code": "ERN",
                    "value": 15
                    },
                    "ETB": {
                    "code": "ETB",
                    "value": 120.6168050953
                    },
                    "ETH": {
                    "code": "ETH",
                    "value": 0.000374365
                    },
                    "EUR": {
                    "code": "EUR",
                    "value": 0.9246500938
                    },
                    "FJD": {
                    "code": "FJD",
                    "value": 2.2392002987
                    },
                    "FKP": {
                    "code": "FKP",
                    "value": 0.7703153383
                    },
                    "GBP": {
                    "code": "GBP",
                    "value": 0.7704001016
                    },
                    "GEL": {
                    "code": "GEL",
                    "value": 2.724550525
                    },
                    "GGP": {
                    "code": "GGP",
                    "value": 0.7703151277
                    },
                    "GHS": {
                    "code": "GHS",
                    "value": 15.9197128215
                    },
                    "GIP": {
                    "code": "GIP",
                    "value": 0.7703152464
                    },
                    "GMD": {
                    "code": "GMD",
                    "value": 57.0669957099
                    },
                    "GNF": {
                    "code": "GNF",
                    "value": 8616.8945933099
                    },
                    "GTQ": {
                    "code": "GTQ",
                    "value": 7.7177512262
                    },
                    "GYD": {
                    "code": "GYD",
                    "value": 208.8535151338
                    },
                    "HKD": {
                    "code": "HKD",
                    "value": 7.7724514764
                    },
                    "HNL": {
                    "code": "HNL",
                    "value": 24.9047842625
                    },
                    "HRK": {
                    "code": "HRK",
                    "value": 6.5218511075
                    },
                    "HTG": {
                    "code": "HTG",
                    "value": 133.4648405221
                    },
                    "HUF": {
                    "code": "HUF",
                    "value": 370.9904724692
                    },
                    "IDR": {
                    "code": "IDR",
                    "value": 15473.800296792
                    },
                    "ILS": {
                    "code": "ILS",
                    "value": 3.7844206408
                    },
                    "IMP": {
                    "code": "IMP",
                    "value": 0.7703150545
                    },
                    "INR": {
                    "code": "INR",
                    "value": 84.0541806707
                    },
                    "IQD": {
                    "code": "IQD",
                    "value": 1308.161891525
                    },
                    "IRR": {
                    "code": "IRR",
                    "value": 42003.13497723
                    },
                    "ISK": {
                    "code": "ISK",
                    "value": 137.7282487198
                    },
                    "JEP": {
                    "code": "JEP",
                    "value": 0.770314838
                    },
                    "JMD": {
                    "code": "JMD",
                    "value": 158.3427260517
                    },
                    "JOD": {
                    "code": "JOD",
                    "value": 0.71
                    },
                    "JPY": {
                    "code": "JPY",
                    "value": 150.6171116397
                    },
                    "KES": {
                    "code": "KES",
                    "value": 128.994687217
                    },
                    "KGS": {
                    "code": "KGS",
                    "value": 85.7506164551
                    },
                    "KHR": {
                    "code": "KHR",
                    "value": 4055.1075819148
                    },
                    "KMF": {
                    "code": "KMF",
                    "value": 453.6394358747
                    },
                    "KPW": {
                    "code": "KPW",
                    "value": 899.9896426566
                    },
                    "KRW": {
                    "code": "KRW",
                    "value": 1375.1562792954
                    },
                    "KWD": {
                    "code": "KWD",
                    "value": 0.3059600362
                    },
                    "KYD": {
                    "code": "KYD",
                    "value": 0.83333
                    },
                    "KZT": {
                    "code": "KZT",
                    "value": 481.7330299609
                    },
                    "LAK": {
                    "code": "LAK",
                    "value": 21931.098708509
                    },
                    "LBP": {
                    "code": "LBP",
                    "value": 89542.549698185
                    },
                    "LKR": {
                    "code": "LKR",
                    "value": 293.6958355392
                    },
                    "LRD": {
                    "code": "LRD",
                    "value": 192.5455939174
                    },
                    "LSL": {
                    "code": "LSL",
                    "value": 17.621922029
                    },
                    "LTC": {
                    "code": "LTC",
                    "value": 0.0141343401
                    },
                    "LTL": {
                    "code": "LTL",
                    "value": 3.1924808693
                    },
                    "LVL": {
                    "code": "LVL",
                    "value": 0.6498132013
                    },
                    "LYD": {
                    "code": "LYD",
                    "value": 4.8057505525
                    },
                    "MAD": {
                    "code": "MAD",
                    "value": 9.8961513755
                    },
                    "MATIC": {
                    "code": "MATIC",
                    "value": 2.6665728978
                    },
                    "MDL": {
                    "code": "MDL",
                    "value": 17.6943325217
                    },
                    "MGA": {
                    "code": "MGA",
                    "value": 4604.2593539886
                    },
                    "MKD": {
                    "code": "MKD",
                    "value": 56.6188606027
                    },
                    "MMK": {
                    "code": "MMK",
                    "value": 2098.1267544242
                    },
                    "MNT": {
                    "code": "MNT",
                    "value": 3410.2121454302
                    },
                    "MOP": {
                    "code": "MOP",
                    "value": 8.0221408753
                    },
                    "MRO": {
                    "code": "MRO",
                    "value": 356.999828
                    },
                    "MRU": {
                    "code": "MRU",
                    "value": 39.5985682368
                    },
                    "MUR": {
                    "code": "MUR",
                    "value": 45.8535375708
                    },
                    "MVR": {
                    "code": "MVR",
                    "value": 15.4490723856
                    },
                    "MWK": {
                    "code": "MWK",
                    "value": 1733.2633072593
                    },
                    "MXN": {
                    "code": "MXN",
                    "value": 19.9601728825
                    },
                    "MYR": {
                    "code": "MYR",
                    "value": 4.3046806545
                    },
                    "MZN": {
                    "code": "MZN",
                    "value": 63.5730682147
                    },
                    "NAD": {
                    "code": "NAD",
                    "value": 17.5777725511
                    },
                    "NGN": {
                    "code": "NGN",
                    "value": 1651.7545245455
                    },
                    "NIO": {
                    "code": "NIO",
                    "value": 36.6973732659
                    },
                    "NOK": {
                    "code": "NOK",
                    "value": 10.9479116444
                    },
                    "NPR": {
                    "code": "NPR",
                    "value": 134.8222876612
                    },
                    "NZD": {
                    "code": "NZD",
                    "value": 1.6602802258
                    },
                    "OMR": {
                    "code": "OMR",
                    "value": 0.383820041
                    },
                    "OP": {
                    "code": "OP",
                    "value": 0.581079407
                    },
                    "PAB": {
                    "code": "PAB",
                    "value": 0.9991101082
                    },
                    "PEN": {
                    "code": "PEN",
                    "value": 3.7665806855
                    },
                    "PGK": {
                    "code": "PGK",
                    "value": 3.9475407392
                    },
                    "PHP": {
                    "code": "PHP",
                    "value": 57.6085966229
                    },
                    "PKR": {
                    "code": "PKR",
                    "value": 277.6043948982
                    },
                    "PLN": {
                    "code": "PLN",
                    "value": 3.9927604266
                    },
                    "PYG": {
                    "code": "PYG",
                    "value": 7950.8804603375
                    },
                    "QAR": {
                    "code": "QAR",
                    "value": 3.6388307212
                    },
                    "RON": {
                    "code": "RON",
                    "value": 4.5975109084
                    },
                    "RSD": {
                    "code": "RSD",
                    "value": 107.7379524398
                    },
                    "RUB": {
                    "code": "RUB",
                    "value": 96.5401271526
                    },
                    "RWF": {
                    "code": "RWF",
                    "value": 1350.2182576422
                    },
                    "SAR": {
                    "code": "SAR",
                    "value": 3.7492204597
                    },
                    "SBD": {
                    "code": "SBD",
                    "value": 8.373396863
                    },
                    "SCR": {
                    "code": "SCR",
                    "value": 15.1419928904
                    },
                    "SDG": {
                    "code": "SDG",
                    "value": 601.5
                    },
                    "SEK": {
                    "code": "SEK",
                    "value": 10.5583915259
                    },
                    "SGD": {
                    "code": "SGD",
                    "value": 1.3165301803
                    },
                    "SHP": {
                    "code": "SHP",
                    "value": 0.7704001408
                    },
                    "SLL": {
                    "code": "SLL",
                    "value": 22647.496367307
                    },
                    "SOL": {
                    "code": "SOL",
                    "value": 0.0060017773
                    },
                    "SOS": {
                    "code": "SOS",
                    "value": 571.1361102433
                    },
                    "SRD": {
                    "code": "SRD",
                    "value": 33.1617441033
                    },
                    "STD": {
                    "code": "STD",
                    "value": 22548.281519821
                    },
                    "STN": {
                    "code": "STN",
                    "value": 22.5482602645
                    },
                    "SVC": {
                    "code": "SVC",
                    "value": 8.75
                    },
                    "SYP": {
                    "code": "SYP",
                    "value": 13026.588845607
                    },
                    "SZL": {
                    "code": "SZL",
                    "value": 17.6123625179
                    },
                    "THB": {
                    "code": "THB",
                    "value": 33.5095858731
                    },
                    "TJS": {
                    "code": "TJS",
                    "value": 10.7189411112
                    },
                    "TMT": {
                    "code": "TMT",
                    "value": 3.5
                    },
                    "TND": {
                    "code": "TND",
                    "value": 3.0833104227
                    },
                    "TOP": {
                    "code": "TOP",
                    "value": 2.3447303345
                    },
                    "TRY": {
                    "code": "TRY",
                    "value": 34.2543350599
                    },
                    "TTD": {
                    "code": "TTD",
                    "value": 6.7807907663
                    },
                    "TWD": {
                    "code": "TWD",
                    "value": 32.0918035727
                    },
                    "TZS": {
                    "code": "TZS",
                    "value": 2717.9399825705
                    },
                    "UAH": {
                    "code": "UAH",
                    "value": 41.35591505
                    },
                    "UGX": {
                    "code": "UGX",
                    "value": 3665.6605033901
                    },
                    "USD": {
                    "code": "USD",
                    "value": 1
                    },
                    "USDC": {
                    "code": "USDC",
                    "value": 0.9975195531
                    },
                    "USDT": {
                    "code": "USDT",
                    "value": 0.9982323441
                    },
                    "UYU": {
                    "code": "UYU",
                    "value": 41.5821044287
                    },
                    "UZS": {
                    "code": "UZS",
                    "value": 12828.15504155
                    },
                    "VEF": {
                    "code": "VEF",
                    "value": 3913208.6494022
                    },
                    "VES": {
                    "code": "VES",
                    "value": 39.132059472
                    },
                    "VND": {
                    "code": "VND",
                    "value": 25275.577675472
                    },
                    "VUV": {
                    "code": "VUV",
                    "value": 119.7587970337
                    },
                    "WST": {
                    "code": "WST",
                    "value": 2.7475604698
                    },
                    "XAF": {
                    "code": "XAF",
                    "value": 606.4491582359
                    },
                    "XAG": {
                    "code": "XAG",
                    "value": 0.0295434415
                    },
                    "XAU": {
                    "code": "XAU",
                    "value": 0.0003674821
                    },
                    "XCD": {
                    "code": "XCD",
                    "value": 2.7
                    },
                    "XDR": {
                    "code": "XDR",
                    "value": 0.7525901366
                    },
                    "XOF": {
                    "code": "XOF",
                    "value": 606.4491258917
                    },
                    "XPD": {
                    "code": "XPD",
                    "value": 0.000946589
                    },
                    "XPF": {
                    "code": "XPF",
                    "value": 110.2571804642
                    },
                    "XPT": {
                    "code": "XPT",
                    "value": 0.0009932235
                    },
                    "XRP": {
                    "code": "XRP",
                    "value": 1.8299587706
                    },
                    "YER": {
                    "code": "YER",
                    "value": 249.788958224
                    },
                    "ZAR": {
                    "code": "ZAR",
                    "value": 17.6171431209
                    },
                    "ZMK": {
                    "code": "ZMK",
                    "value": 9001.2
                    },
                    "ZMW": {
                    "code": "ZMW",
                    "value": 26.7408845423
                    },
                    "ZWL": {
                    "code": "ZWL",
                    "value": 67470.705157091
                    }
                }
                }
        
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
                                'type': 'currency',
                                'row_id': row['row_id'],
                                'Date': today
                            }
                            errors_list.append(err)

                error_df = pd.DataFrame(errors_list)
                error_df.to_sql('error_logs', ps_engine, if_exists='append', index=False) 
            
            # ms_hook = MsSqlHook(mssql_conn_id='local_mssql', schema='AdventureWorks2022')
            # print(ms_hook.get_sqlalchemy_engine())
            # flattened_dataframe.to_sql('currency_stage', ms_hook.get_sqlalchemy_engine(), if_exists='append', index=False)

        api_response = hit_currency_api()
        load_currency_data(flatten_market_data(api_response))             
    

# Prints data about all DAGs in your Deployment