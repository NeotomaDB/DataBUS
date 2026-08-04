import csv
import logging

import openpyxl


def read_xlsx(filename, num_headers=1):
    """Read an Excel file and return a dict mapping sheet names to rows.

    Each sheet is parsed into a list of dictionaries using the header row(s)
    as column names. FAIRe sheets (3-row header: requirement level, section,
    ``term_name``) are detected per sheet and read from the ``term_name`` row.
    Otherwise, when ``num_headers >= 2``, sheets whose second row contains at
    least one ``None`` value are treated as having multi-row headers: the first
    two rows are joined with ``_`` to form combined column names (e.g.
    ``TaxonName`` + ``ASV1`` → ``TaxonName_ASV1``). Sheets where the second row
    has no ``None`` values are assumed to have a single header row (the second
    row is treated as data).

    Examples:
        >>> read_xlsx('data.xlsx')  # doctest: +SKIP
        {'Site': [{'site_name': 'Lake X', 'lat': '45.0'}], 'Samples': [...]}

    Args:
        filename (str): Path to the .xlsx file to read.
        num_headers (int): Number of header rows declared in the template.
            When >= 2, multi-row header combining is attempted per sheet.
            Defaults to 1.

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
        if _is_faire_header(rows):
            headers = _single_header(rows[2])
            data_start = 3
        elif num_headers >= 2 and len(rows) >= 2 and any(v is None for v in rows[1]):
            headers = _combine_header_rows(rows[0], rows[1])
            data_start = _stacked_data_start(rows)
        else:
            headers = _single_header(rows[0])
            data_start = 1
        result[sheet_name] = [
            dict(zip(headers, row, strict=False))
            for row in rows[data_start:]
            if any(v is not None for v in row)
        ]
    wb.close()
    return result


def _combine_header_rows(row0, row1):
    """Combine two header rows into a single list of column names.

    For columns where row1 is ``None`` or empty, uses row0 alone.
    For columns where both rows have values, joins them with ``_``.

    Args:
        row0 (tuple): First header row values.
        row1 (tuple): Second header row values.

    Returns:
        list[str]: Combined column names.
    """
    headers = []
    for i, (h0, h1) in enumerate(zip(row0, row1, strict=False)):
        if h0 is None:
            headers.append(f"col_{i}")
        elif h1 is None or str(h1).strip() == "":
            headers.append(str(h0))
        else:
            headers.append(f"{h0}_{h1}")
    return headers


def _single_header(row):
    """Turn one header row into column names, filling blanks with ``col_<i>``."""
    return _combine_header_rows(row, [None] * len(row))


def _stacked_data_start(rows):
    """First data-row index for a stacked taxon header (name / ASV / sequence).

    Sub-header rows only populate the taxon columns — the same positions that
    are ``None`` in row 1. The first row that also fills a non-taxon column is
    the first data row. Falls back to ``len(rows)`` when none qualifies.
    """
    sparse = [i for i, v in enumerate(rows[1]) if v is None]
    for idx in range(1, len(rows)):
        if not all(rows[idx][i] is None for i in sparse):
            return idx
    return len(rows)


_FAIRE_LEVELS = {"m", "o", "r", "hr"}


def _is_faire_header(rows):
    """Detect a 3-row FAIRe header (requirement level / section / term_name).

    True when the sheet has at least three rows and the first row is a row of
    FAIRe requirement-level codes (``M``/``O``/``R``/``HR``), optionally led by
    the ``requirement_level_code`` label cell. The real column names then live
    on the third row (``term_name``).
    """
    if len(rows) < 3:
        return False
    values = [str(v).strip() for v in rows[0] if v is not None]
    if not values:
        return False
    if values[0].lstrip("# ").lower() == "requirement_level_code":
        values = values[1:]
    return bool(values) and all(v.lstrip("# ").lower() in _FAIRE_LEVELS for v in values)


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
