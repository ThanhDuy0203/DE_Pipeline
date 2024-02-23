from airflow.exceptions import AirflowException
from dataUltis.handler.blob_storage_handler import BlobStorageHandler
from dataUltis.basic.table_utils import TableUtils
from dataUltis.basic.blob_packet import BlobPacket
from dataUltis.handler.json_file_handler import JsonFileHandler

class SnowflakeHandler:
    """
    Handles operations related to Snowflake.
    """
    def __init__(self, snowflake_conn_id, blob_storage_handler: BlobStorageHandler, json_file_handler: JsonFileHandler):
        self.snowflake_conn_id = snowflake_conn_id
        self.blob_storage_handler = blob_storage_handler
        self.json_file_handler = json_file_handler

    def create_extract_query_direct(self, table_lake: str, table_target: str, module_name: str,
                                    column_mapping: str, blob_packet: BlobPacket, blob_file_path=None):
        """
        Creates a Snowflake SQL query for extracting data from Azure Blob Storage.

        Args:
            table_lake (str): The folder (and also the table name) of the lake table.
            table_target (str): The target table that will be updated to in Snowflake.
            module_name (str): The module name.
            column_mapping (str): The column mapping.
            blob_packet (BlobPacket): An object containing Azure Blob Storage account information.
            blob_file_path (str): The Azure Blob Storage path to search for the latest blob.

        Returns:
            str: Snowflake SQL query.
        """
        
        if blob_file_path is None:
            blob_file_path = TableUtils.premade_blob_file_path(table_lake)
        
        blob_name = self.blob_storage_handler.get_latest_blob(blob_packet, blob_file_path)
        table_lake = TableUtils.check_table_name(table_lake)
        table_name = TableUtils.check_table_name(table_target)
        
        # Delete and then adding back new data, it's like doing a loop two times. Yeez, I wonder how long this will take if the table has like 1mil records.
        # But it ran, so it's good enough. I'll deal with this later.

        # This query is gonna connect to Azure Blob Storage, delete all data in target table and re-add new data into the table.
        # This stuff is very inefficient imo and only should be used if the data in the table isn't that much to begin with.
        snowflake_query = f"""
                        USE DATABASE {module_name};
                        USE SCHEMA {module_name}_BRONZE;
                        CREATE OR REPLACE TEMP STAGE {table_name}_{module_name}_stage
                        URL= 'azure://{blob_packet.account_url.replace("https://", '')}{blob_packet.container_name}/{blob_name}'
                        CREDENTIALS=(AZURE_SAS_TOKEN='{blob_packet.credential}')
                        FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE SKIP_HEADER=0 SKIP_BLANK_LINES=TRUE);

                        CREATE OR REPLACE TEMP FILE FORMAT {table_name}_{module_name}_csv_ffs
                            TYPE = CSV
                            PARSE_HEADER = TRUE;

                        CREATE OR REPLACE TEMP TABLE {table_name}_{module_name}_temp_table
                        USING TEMPLATE
                        (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                            LOCATION=>'@{table_name}_{module_name}_stage'
                            , FILE_FORMAT=>'{table_name}_{module_name}_csv_ffs'
                            )
                            ));

                        COPY INTO {table_name}_{module_name}_temp_table
                        FROM @{table_name}_{module_name}_stage
                        FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1);

                        DELETE FROM {table_name};
                        COPY INTO {table_name}
                        FROM (
                            SELECT {column_mapping}
                            FROM {table_name}_{module_name}_temp_table
                        ) FORCE=TRUE;
                        """
        return snowflake_query
    
    def create_extract_query_upsert(self, table_lake: str, table_target: str, module_name: str,
                                    json_file_name: str, blob_packet: BlobPacket,
                                    primary_key=None, blob_file_path=None):
        """
        Creates a Snowflake SQL query for extracting data from Azure Blob Storage using UPSERT method.

        Args:
            table_lake (str): The folder (and also the table name) of the lake table.
            table_target (str): The target table that will be updated to in Snowflake.
            module_name (str): The module name.
            column_mapping (str): The column mapping.
            blob_packet (BlobPacket): An object containing Azure Blob Storage account information.
            primary_key (str): The primary key colume name
            blob_file_path (str): The Azure Blob Storage path to search for the latest blob.

        Returns:
            str: Snowflake SQL query.
        """
        if primary_key is None:
            raise AirflowException("Primary key name needed if using upsert_mode")

        if blob_file_path is None:
            blob_file_path = TableUtils.premade_blob_file_path(table_lake)
        
        table_lake = TableUtils.check_table_name(table_lake)
        table_name = TableUtils.check_table_name(table_target)
        blob_name = self.blob_storage_handler.get_latest_blob(blob_packet, blob_file_path)
        
        column_mapping = self.json_file_handler.read_json_into_mapping_str(json_file_name)
        extract_name = [chunk.split(" ")[2] for chunk in column_mapping.split(', ')]
        set_new_value = (", ").join([f"{column_name} = table_source.{column_name}" for column_name in extract_name])
        new_value = (", ").join([f"table_source.{column_name}" for column_name in extract_name])

        # This query will connect the Azure Blob Storage and try to upsert new data by the chosen primary key.
        snowflake_query = f"""
                        USE DATABASE {module_name};
                        USE SCHEMA {module_name}_BRONZE;
                        CREATE OR REPLACE TEMP STAGE {table_name}_{module_name}_stage
                        URL= 'azure://{blob_packet.account_url.replace("https://", '')}{blob_packet.container_name}/{blob_name}'
                        CREDENTIALS=(AZURE_SAS_TOKEN='{blob_packet.credential}')
                        FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE SKIP_HEADER=0 SKIP_BLANK_LINES=TRUE);

                        CREATE OR REPLACE TEMP FILE FORMAT {table_name}_{module_name}_csv_ffs
                            TYPE = CSV
                            PARSE_HEADER = TRUE;

                        CREATE OR REPLACE TEMP TABLE {table_name}_{module_name}_temp_table
                        USING TEMPLATE
                        (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                            LOCATION=>'@{table_name}_{module_name}_stage'
                            , FILE_FORMAT=>'{table_name}_{module_name}_csv_ffs'
                            )
                            ));

                        COPY INTO {table_name}_{module_name}_temp_table
                        FROM @{table_name}_{module_name}_stage
                        FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1);

                        MERGE INTO {table_name} AS table_target USING
                        (
                            SELECT {column_mapping}
                            FROM {table_name}_{module_name}_temp_table
                        ) table_source ON table_target.{primary_key} = table_source.{primary_key}
                        WHEN MATCHED THEN
                        UPDATE SET 
                            {set_new_value}
                        WHEN NOT MATCHED THEN
                        INSERT
                            ({(", ").join(extract_name)}) VALUES
                            ({new_value});
                        """
        
        return snowflake_query