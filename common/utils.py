import os
import json
from typing import Optional, Dict, Any


def store_json_file(data: Dict[str, Any], path: str, file_name: str) -> Optional[str]:
    """
    Store extracted JSON data in a file.

    :param data: Content of the JSON file.
    :param path: Storage location, e.g. /Volumes/dev/.
    :param file_name: Name of the file to be saved.
    :return: Error message as a string if an error occurs, None otherwise.
    """
    try:
        if not os.path.exists(path):
            os.makedirs(path)

        file_path = os.path.join(path, f"{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        
        return None  # No error occurred, return None
    except FileNotFoundError:
        return f"Specified path '{path}' does not exist."
    except PermissionError:
        return f"Permission denied while writing to '{file_path}'."
    except OSError as e:
        return f"Error occurred while writing to '{file_path}': {e}"
    except Exception as e:
        return f"An unexpected error occurred: {e}"