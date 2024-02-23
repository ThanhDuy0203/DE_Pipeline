"""
DAG for extracting data from MSSQL to ADLS.
This DAG extracts data from a Microsoft Server SQL Database and push it into an Azure Blob Storage.
Note: Update the configuration parameters in the Variable section.
"""

import pendulum

from airflow import DAG
from airflow.models import Variable

from dataUltis.handler.blob_storage_handler import BlobStorageHandler
from dataUltis.handler.sql_file_handler import SqlFileHandler
from dataUltis.handler.mssql_handler import MSSQLHandler
from dataUltis.ultis.data_extractor import DataExtractor

AZURE_CONN_ID = Variable.get("LAKE_ADLS_CONN_ID")
MSSQL_CONN_ID = Variable.get("SOURCE_MSSQL_CONN_ID")

SQL_PATH_FILE = './include/sql/'
CSV_PATH_FILE = './include/temp/'

table_source = "Application.PaymentMethods"
query_file_name = "paymentmethods_query.sql"

blob_handler = BlobStorageHandler(AZURE_CONN_ID, SQL_PATH_FILE, CSV_PATH_FILE)
sql_handler = SqlFileHandler(SQL_PATH_FILE)
mssql_handler = MSSQLHandler(MSSQL_CONN_ID, CSV_PATH_FILE, sql_handler)
data_extractor = DataExtractor(source=mssql_handler, target=blob_handler)

with DAG(
    dag_id=f'pl_lake_{table_source}_extract',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    extract_source_into_local_task = data_extractor.create_mssql_dag_task_local(dag, table_source, query_file_name)
    upload_local_into_blob_task = data_extractor.create_blob_dag_task_upload(dag, table_source, None)
    
    extract_source_into_local_task >> upload_local_into_blob_task