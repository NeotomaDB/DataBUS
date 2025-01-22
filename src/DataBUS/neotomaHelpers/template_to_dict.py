import yaml
from yaml.loader import SafeLoader
import os
from .excel_to_yaml import excel_to_yaml


def template_to_dict(temp_file):
    """_Read in valid .xlsx or .yml file._

    Args:
        temp_file (_string_): _A valid filename for a yml or xlsx file._

    Returns:
        _dict_: _A dict representation of the template file._
    """
    if not os.path.isfile(temp_file):
        raise FileNotFoundError(
            f"The file '{temp_file}' could not be found within the current path."
        )

    file_name, file_extension = os.path.splitext(temp_file)

    if file_extension.lower() == ".yml" or file_extension.lower() == ".yaml":
        with open(temp_file, encoding="UTF-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        return data

    elif file_extension.lower() == ".xls" or file_extension.lower() == ".xlsx":
        # Use pandas to read the Excel file and convert it to a dictionary
        # data = pd.read_excel(temp_file)
        excel_to_yaml(temp_file, file_name)
        file_name = file_name + ".yml"

        with open(file_name, encoding="UTF-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        return data

    else:
        raise ValueError(
            f"Unsupported file type: {file_extension}. Only .yml, .yaml, .xls, and .xlsx are supported."
        )
