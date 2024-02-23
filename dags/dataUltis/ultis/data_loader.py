from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dataUltis.handler.snowflake_handler import SnowflakeHandler
from dataUltis.basic.blob_packet import BlobPacket

class DataLoader:
    """
    Extracts data from lake into data warehouse
    """
    def __init__(
        self,
        handler: object
    ):
        self.handler = handler

    def create_blob_snowflake_dag_task_direct(self, dag, table_lake: str, table_target: str,
                                              module_name: str, column_mapping: str, blob_file_path=None):
        """
        Create a SnowflakeOperator task for extracting data directly from a blob to a Snowflake table.
        
        Parameters:
            - dag (DAG): The Airflow DAG to which the task belongs.
            - table_lake (str): The name of the Snowflake table where data will be extracted.
            - module_name (str): The name of the module or dataset.
            - column_mapping (str): The mapping of columns between the blob and Snowflake table.
            - blob_file_path (str, optional): The path to the blob file if applicable.
            
        Returns:
            - SnowflakeOperator: The SnowflakeOperator task for the extraction.
            
        Raises:
            - AirflowException: If the handler is not a SnowflakeHandler object.
        """
        if isinstance(self.handler, SnowflakeHandler):
            blob_packet = self.handler.blob_storage_handler.get_blob_service_account_credential()
            return SnowflakeOperator(
                task_id=f"extract_{table_lake}_to_bronze_direct",
                sql=self.handler.create_extract_query_direct(table_lake, table_target, module_name, column_mapping, blob_packet, blob_file_path),
                dag=dag,
                snowflake_conn_id=self.handler.snowflake_conn_id
            )
        else:
            raise AirflowException('Handler is not a SnowflakeHandler object')
        
    def create_blob_snowflake_dag_task_upsert(self, dag, table_lake: str, table_target: str, module_name: str, json_file_name: str,
                                            primary_key: str, blob_file_path=None):
        """
        Create a SnowflakeOperator task for upserting data from a blob to a Snowflake table.
        
        Parameters:
            - dag (DAG): The Airflow DAG to which the task belongs.
            - table_lake (str): The name of the Snowflake table where data will be upserted.
            - module_name (str): The name of the module or dataset.
            - column_mapping (str): The mapping of columns between the blob and Snowflake table.
            - primary_key (str): The primary key of the Snowflake table.
            - blob_file_path (str, optional): The path to the blob file if applicable.
            
        Returns:
            - SnowflakeOperator: The SnowflakeOperator task for the upsert.
            
        Raises:
            - AirflowException: If the handler is not a SnowflakeHandler object.
        """
        if isinstance(self.handler, SnowflakeHandler):
            blob_packet = self.handler.blob_storage_handler.get_blob_service_account_credential()
            
            return SnowflakeOperator(
                task_id=f"extract_{table_lake}_to_bronze_upsert",
                sql=self.handler.create_extract_query_upsert(table_lake, table_target, module_name, json_file_name,
                                                             blob_packet, primary_key, blob_file_path),
                dag=dag,
                snowflake_conn_id=self.handler.snowflake_conn_id
            )
        else:
            raise AirflowException('Handler is not a SnowflakeHandler object')