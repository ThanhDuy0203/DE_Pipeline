from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dataUltis.handler.mssql_handler import MSSQLHandler
from dataUltis.handler.blob_storage_handler import BlobStorageHandler

class DataExtractor:
    """
    Orchestrates the extraction of data from various sources.
    """
    def __init__(
        self,
        source: object,
        target: object,
    ):
        self.source = source
        self.target = target

    def create_mssql_dag_task_local(self, dag, table_source, query_file_name):
        """
        Creates a DAG task for extracting data from Microsoft SQL Server.

        Args:
            dag (DAG): The parent DAG.
            table_source (str): The source table.
            query_file_name (str): The name of the SQL file.

        Returns:
            PythonOperator: DAG task.
        """
        if isinstance(self.source, MSSQLHandler):
            return PythonOperator(
                task_id=f"extract_{table_source}_to_local",
                python_callable=self.source.extract_data,
                dag=dag,
                op_kwargs={"table_source": table_source, "query_file_name": query_file_name},
            )
        else:
            raise AirflowException('Source object is not suitable for MSSQL')
        
    def create_blob_dag_task_upload(self, dag, table_source, blob_file_path):
        """
        Creates a DAG task for uploading data to Azure Blob Storage.

        Args:
            dag (DAG): The parent DAG.
            table_source (str): The source table.
            blob_file_path (str): The Azure Blob Storage path.

        Returns:
            PythonOperator: DAG task.
        """
        if isinstance(self.target, BlobStorageHandler):
            self.target.get_blob_service_account_credential()
            blob_serivce_client = self.target.get_blob_service_client()[0]

            return PythonOperator(
                task_id=f"upload_{table_source}_to_adls",
                python_callable=self.target.upload_file_to_azure,
                dag=dag,
                op_kwargs={"blob_service_client": blob_serivce_client,
                           "table_source": table_source,
                           "blob_file_path": blob_file_path
                }
            )
        else:
            raise AirflowException('Target object is not suitable for BlobHandler')