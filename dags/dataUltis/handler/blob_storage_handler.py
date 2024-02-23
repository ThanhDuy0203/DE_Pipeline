import os

from dataUltis.basic.blob_packet import BlobPacket
from dataUltis.basic.table_utils import TableUtils

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from azure.storage.blob import BlobServiceClient

class BlobStorageHandler:
    """
    Handles operations related to Azure Blob Storage.
    """
    def __init__(
        self, azure_conn_name, sql_path_file, csv_path_file
    ):
        self.azure_conn_name = azure_conn_name
        self.sql_path_file = sql_path_file
        self.csv_path_file = csv_path_file
        self.blob_packet = None

    def get_blob_service_account_credential(self):
        """
        Retrieves Azure Blob Storage account credentials from the Airflow connection.

        Returns:
            BlobPacket: An object containing account information.
        """
        azure_hook = BaseHook.get_connection(conn_id=self.azure_conn_name)
        account_url = azure_hook.host

        container_name = account_url.split("/")[3]
        account_url = account_url[: len(account_url) - len(container_name)]
        credential = azure_hook.extra_dejson["shared_access_key"]

        self.blob_packet = BlobPacket(account_url, credential, container_name)

        return self.blob_packet

    def get_blob_service_client(self):
        """
        Creates and returns a BlobServiceClient using the provided BlobPacket.

        Returns:
            BlobServiceClient: Azure Blob Service client.
            str: Container name.
        """
        if self.blob_packet is None:
            return None
        
        account_url = self.blob_packet.account_url
        credential = self.blob_packet.credential
        container_name = self.blob_packet.container_name

        blob_service_client = BlobServiceClient(account_url, credential=credential)

        return blob_service_client, container_name

    def get_latest_blob(self, blob_packet, blob_file_path):
        """
        Retrieves the latest blob in the specified Azure Blob Storage path.

        Args:
            blob_packet (BlobPacket): An object containing Azure Blob Storage account information.
            blob_file_path (str): The Azure Blob Storage path to search for the latest blob.

        Returns:
            str: Name of the latest blob in the specified path.

        Raises:
            AirflowException: If no blobs are found in the specified path.
        """
        blob_service_client = self.get_blob_service_client()[0]
        container_client = blob_service_client.get_container_client(
            container=blob_packet.container_name
        )

        blobs = list(container_client.walk_blobs(name_starts_with=blob_file_path))
        blob_info = [(blob.name, blob.last_modified) for blob in blobs]

        if blob_info:
            latest_blob = max(blob_info, key=lambda x: x[1])
            return latest_blob[0]
        else:
            raise AirflowException(f"No blob with file path {blob_file_path} or no file found in {blob_file_path}")
        
    def upload_file_to_azure(
        self,
        blob_service_client,
        table_source,
        blob_file_path=None,
    ):
        """
        Uploads files into Azure Blob Storage using the BlobServiceClient.

        Args:
            blob_service_client (BlobServiceClient): Azure Blob Service client.
            table_source (str): The source table.
            blob_file_path (str, optional): Custom blob file path. Defaults to None.
        """
        container_client = blob_service_client.get_container_client(
            container=self.blob_packet.container_name
        )   

        table_name = TableUtils.check_table_name(table_source)
        table_file_name = TableUtils.premade_table_csv_name(table_name)
        full_file_path = os.path.join(os.path.abspath(self.csv_path_file), table_file_name)
        
        if blob_file_path is None:
            blob_file_path = TableUtils.premade_blob_file_path(table_name) + table_file_name
            
        if not os.path.exists(full_file_path):
            raise AirflowException(f"No file {full_file_path} to push.")

        try:
            with open(full_file_path, "rb") as data:
                blob_client = container_client.upload_blob(
                    name=blob_file_path, data=data, overwrite=True
                )
                os.remove(full_file_path)
        except Exception as e:
            raise AirflowException(full_file_path, blob_file_path)

        return True
