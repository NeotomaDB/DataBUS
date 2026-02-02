import csv


def read_csv(filename):
    """Read CSV file and return a structured list of dictionaries.

    Parses a CSV file and converts each row into a dictionary with column headers
    as keys.

    Examples:
        >>> read_csv('pollen_data.csv')  # doctest: +SKIP
        [{'depth': '2.5', 'quercus': '125'}, {'depth': '5.0', 'quercus': '142'}]
        >>> read_csv('chronology.csv')  # doctest: +SKIP
        [{'depth': '10.0', 'age': '3250', 'error': '100'}]

    Args:
        filename (str): Path to the CSV file to read.

    Returns:
        list: List of dictionaries where each dictionary represents a row,
              with column headers as keys.
    """
    # SUGGESTION: Consider adding error handling for file not found and CSV parsing errors
    with open(filename) as f:
        file_data = csv.reader(f)
        headers = next(file_data)
        return [dict(zip(headers, i)) for i in file_data]
