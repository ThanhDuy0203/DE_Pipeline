import pendulum

from airflow.exceptions import AirflowException

class TableUtils:
    """
    Utility class for table-related operations.
    """
    @staticmethod
    def check_table_name(table_name):
        """
        Extracts and validates the table name from the input.

        Args:
            table_name (str): The input table name, optionally with schema.

        Returns:
            str: Extracted table name.

        Raises:
            AirflowException: If the table_name is None.
        """
        if table_name is None:
            raise AirflowException("Need table name for extract!")
        elif table_name.count(".") == 2:
            return table_name.split(".")[2]
        elif table_name.count(".") == 1:
            return table_name.split(".")[1]
        else:
            return table_name

    @staticmethod
    def premade_table_csv_name(table_name):
        """
        Generates a CSV file name based on the provided table name.

        Args:
            table_name (str): The table name.

        Returns:
            str: CSV file name.
        """
        return "{}_{}.csv".format(
            table_name.lower(), pendulum.now().format("YYYY_MM_DD")
        )

    @staticmethod
    def premade_blob_file_path(table_name):
        """
        Generates a default Azure Blob file path based on the provided table name.

        Args:
            table_name (str): The table name.

        Returns:
            str: Default Azure Blob file path.
        """
        table_name = TableUtils.check_table_name(table_name)
        return f'/landing/{table_name.lower()}/'
