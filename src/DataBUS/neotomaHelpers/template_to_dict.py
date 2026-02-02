import yaml
from yaml.loader import SafeLoader
import os
from .excel_to_yaml import excel_to_yaml

def template_to_dict(temp_file):
    """Convert YAML or XLSX template file to Python dictionary.

    Reads and parses template files in YAML or XLSX format, converting Excel files
    to YAML format first if necessary. Supports .yml, .yaml, .xls, and .xlsx formats.

    Examples:
        >>> template_to_dict('pollen_template.yml')  # doctest: +SKIP
        {'apiVersion': 'neotoma v2.0', 'metadata': [...], 'kind': 'Dataset'}
        >>> template_to_dict('chronology_template.xlsx')  # doctest: +SKIP
        {'apiVersion': 'neotoma v2.0', 'metadata': [...], 'kind': 'Chronology'}

    Args:
        temp_file (str): Path to a valid yml, yaml, xls, or xlsx template file.

    Returns:
        dict: Dictionary representation of the template file with 'apiVersion',
              'headers', 'kind', and 'metadata' keys.

    Raises:
        FileNotFoundError: If the specified template file does not exist.
        ValueError: If the file extension is not one of the supported types.
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
        excel_to_yaml(temp_file, file_name)
        file_name = file_name + ".yml"
        # Clean up the generated YAML file after reading it
        with open(file_name, encoding="UTF-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        return data

    else:
        raise ValueError(
            f"Unsupported file type: {file_extension}. Only .yml, .yaml, .xls, and .xlsx are supported."
        )