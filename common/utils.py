from typing import Optional
import os 
import json

def store_json_file(data: str, path: str, file_name: str) -> Optional[str]:
    """
    Store extracted JSON data in a file.

    :param
        data: Content of the json file.
        path: Storage location, e.g. /Volumes/dev/
        file_name: Name of the file to be saved.
    :return: Error message as a string or None if successful.
    """
    if not os.path.exists(path):
        os.makedirs(path)

    with open(f"{path}/{file_name}.json", "w", encoding="utf-8") as f:
        try:
            json.dump(data, f)
        except Exception as e:
            return str(e)