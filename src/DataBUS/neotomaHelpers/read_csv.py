import csv
import logging

def read_csv(filename):
    """Read CSV file and return a structured list of dictionaries.

    Parses a CSV file and converts each row into a dictionary with column headers
    as keys.

    Examples:
        >>> read_csv('pollen_data.csv')  # doctest: +SKIP
        [{'depth': '2.5', 'quercus': '125'}, {'depth': '5.0', 'quercus': '142'}]

    Args:
        filename (str): Path to the CSV file to read.

    Returns:
        list: List of dictionaries where each dictionary represents a row,
              with column headers as keys.
    """
    with open(filename) as f:
        try:
            file_data = csv.reader(f)
            headers = next(file_data)
        except FileNotFoundError:
            logging.error(f"CSV file not found: {filename}")
            return []
        except StopIteration:
            logging.error(f"CSV file is empty: {filename}")
            return []
        except Exception as e:
            logging.error(f"Error reading CSV file {filename}: {e}")
            return []
        
        try:
            return [dict(zip(headers, row)) for row in file_data]
        except Exception as e:
            logging.error(f"Error parsing CSV rows from {filename}: {e}")
            return []