import csv
import logging

import openpyxl


def read_xlsx(filename):
    """Read an Excel file and return a dict mapping sheet names to rows.

    Each sheet is parsed into a list of dictionaries using the first row as
    column headers. Sheets with no data rows are included as empty lists.

    Examples:
        >>> read_xlsx('data.xlsx')  # doctest: +SKIP
        {'Site': [{'site_name': 'Lake X', 'lat': '45.0'}], 'Samples': [...]}

    Args:
        filename (str): Path to the .xlsx file to read.

    Returns:
        dict: Mapping of sheet name (str) to list of row dicts.
    """
    try:
        wb = openpyxl.load_workbook(filename, read_only=True, data_only=True)
    except FileNotFoundError:
        logging.error(f"Excel file not found: {filename}")
        return {}
    except Exception as e:
        logging.error(f"Error opening Excel file {filename}: {e}")
        return {}

    result = {}
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            result[sheet_name] = []
            continue
        headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
        result[sheet_name] = [
            dict(zip(headers, row, strict=False)) for row in rows[1:] if any(v is not None for v in row)
        ]
    wb.close()
    return result


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
            return [dict(zip(headers, row, strict=False)) for row in file_data]
        except Exception as e:
            logging.error(f"Error parsing CSV rows from {filename}: {e}")
            return []
