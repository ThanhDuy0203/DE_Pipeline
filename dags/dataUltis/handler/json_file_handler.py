import json
import os 

from airflow.exceptions import AirflowException

class JsonFileHandler:
    """
    Handles reading SQL files and generating SQL strings for further use.
    """
    def __init__(self, json_path_file: str):
        self.json_path_file = os.path.abspath(json_path_file)

    def read_json_into_mapping_str(self, json_file_name: str):
        file_list = []
        full_file_path = os.path.join(self.json_path_file, json_file_name)
        
        if not os.path.exists(os.path.abspath(self.json_path_file)):
            os.makedirs(os.path.abspath(self.json_path_file))
            
        if not os.path.exists(full_file_path):
            raise AirflowException(f"No specified json file found {full_file_path}")
        
        with open(full_file_path) as f:
            data = json.load(f)

        try:
            for i in data["mapping"]:
                file_list.append(f"{i['source']} AS {i['target']}")
        except Exception as e:
            raise AirflowException("Json object must be named 'Mapping'")
        
        return ", ".join(file_list)