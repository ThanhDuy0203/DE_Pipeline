"""
DAG for extracting data from ADLS to Bronze layer in Snowflake.
This DAG extracts data from an Azure Data Lake Storage account and loads it into the Bronze layer in Snowflake.
Note: Update the configuration parameters in the Variable section.

Warning: the load_blob_into_bronze.sql is crucial for loading the table into Snowflake, DO NOT DELETE THE FILE.
"""

import pendulum

from airflow import DAG
from airflow.models import Variable

from dataUltis.handler.blob_storage_handler import BlobStorageHandler
from dataUltis.handler.snowflake_handler import SnowflakeHandler
from dataUltis.handler.json_file_handler import JsonFileHandler
from dataUltis.ultis.data_loader import DataLoader

AZURE_CONN_ID = Variable.get("LAKE_ADLS_CONN_ID")
SNOWFLAKE_CONN_ID = Variable.get("BRONZE_SNOWFLAKE_CONN_ID")

SQL_PATH_FILE = './include/sql/'
CSV_PATH_FILE = './include/temp/'
JSON_PATH_FILE = './include/json/'

table_lake = "PaymentMethods" #Folder that contains wanted tables from blob
table_target = "Transaction.PaymentMethod"
module_name = "TRANSACTION"

json_file_name = "paymentmethod_mapping.json"
#Only use when upserting into table

json_file_handler = JsonFileHandler(JSON_PATH_FILE)
blob_handler = BlobStorageHandler(AZURE_CONN_ID, SQL_PATH_FILE, CSV_PATH_FILE)
snowflake_handler = SnowflakeHandler(SNOWFLAKE_CONN_ID, blob_handler, json_file_handler)
data_loader = DataLoader(handler=snowflake_handler)

with DAG(
    dag_id=f'pl_bronze_{table_target}_extract',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    get_blob_packet_task = data_loader.create_blob_snowflake_dag_task_upsert(
        dag=dag,
        table_lake=table_lake,
        table_target=table_target,
        module_name=module_name,
        json_file_name=json_file_name,
        primary_key='PAYMENTMETHODID',
        blob_file_path=None
    )