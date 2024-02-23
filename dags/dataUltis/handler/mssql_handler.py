import os
import logging

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from dataUltis.basic.table_utils import TableUtils

class MSSQLHandler:
    """
    Handles operations related to Microsoft SQL Server.
    """
    def __init__(self, mssql_conn_id, csv_path_file, sql_file_handler):
        self.mssql_conn_id = mssql_conn_id
        self.csv_path_file = os.path.abspath(csv_path_file)
        self.sql_file_handler = sql_file_handler

    def extract_data(self, table_source, query_file_name):
        """
        Extracts data from Microsoft SQL Server and saves it to a CSV file.

        Args:
            table_source (str): The source table.
            query_file_name (str): The name of the SQL file.

        Raises:
            AirflowException: If the CSV file cannot be created.
        """
        table_name = TableUtils.check_table_name(table_source)
        file_name = TableUtils.premade_table_csv_name(table_name)

        full_file_path = os.path.join(self.csv_path_file, file_name)

        if not os.path.exists(self.csv_path_file):
            logging.info("Path not exists, create new directory")
            os.makedirs(self.csv_path_file)
                    
        if os.path.exists(full_file_path):
            logging.info(f"File {table_name.lower()} already existed. Removing.")
            os.remove(full_file_path)

        sql_query = self.sql_file_handler.read_sql_into_string(query_file_name)
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        mssql_data_frame = mssql_hook.get_pandas_df(sql=sql_query)
        mssql_data_frame.to_csv(full_file_path, index=False)
        logging.info(f"Finish exporting {table_name.lower()}.csv to folder")

        return True
       
        