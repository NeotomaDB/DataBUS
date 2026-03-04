import re

from . import utils as ut


def pull_params(params, yml_dict, csv_template, table=None):
    """Pull and process parameters for database insert statements.

    Extracts parameters from YAML template and CSV data, performs type conversions
    (date, int, float, coordinates, string), handles special cases like notes and
    chronologies, and returns cleaned data ready for insertion.

    Args:
        params (list): List of strings for columns needed to generate insert statement.
        yml_dict (dict): Dictionary returned by YAML template containing 'metadata' key.
        csv_template (dict): CSV data as list of dictionaries with column data to upload.
        table (str or list, optional): Name of the table(s) parameters are drawn for.
                                      If list, returns results for each table.
        name (str, optional): Name field identifier. Defaults to None.
        values (bool, optional): Whether to treat columns as value columns. Defaults to False.

    Returns:
        dict or list: Cleaned and formatted parameters ready for database insertion.
                      If table is a list, returns list of dicts. If table is str, returns single dict.
                      Returns hierarchical dicts for special tables like chronologies/sampleages.
    """
    if isinstance(table, list):
        return [pull_params(params, yml_dict, csv_template, item) for item in table]

    add_unit_inputs = {}
    if re.match(r".*\.$", table) is None:
        table = table + "."
    params = ut.prepare_parameters(params, yml_dict, table)
    for i in params:
        _process_parameter(i, table, yml_dict, csv_template, add_unit_inputs)
    return ut.finalize_output(add_unit_inputs)


def _process_parameter(param_name, table, yml_dict, csv_template, add_unit_inputs):
    """Process a single parameter, handling all special cases."""
    valor = ut.retrieve_dict(yml_dict, table + param_name)
    if not valor:
        add_unit_inputs[param_name] = None
        return
    for val in valor:
        _process_value_entry(param_name, val, csv_template, table, add_unit_inputs)


def _process_value_entry(param_name, val_entry, csv_template, table, add_unit_inputs):
    """Process a single value entry with type conversion and special case handling."""
    try:
        clean_valor = ut.clean_column(
            val_entry.get("column"), csv_template, clean=not val_entry.get("rowwise")
        )
    except KeyError:
        return
    if not clean_valor:
        if "taxonname" not in val_entry:
            add_unit_inputs[param_name] = None
        return
    clean_valor = ut.convert_value_by_type(val_entry, clean_valor)
    if not clean_valor:
        if "taxonname" not in val_entry:
            add_unit_inputs[param_name] = None
        return
    if param_name == "notes":
        ut.add_note_entry(add_unit_inputs)
    elif any(k in table for k in ("chronologies", "sampleages")):
        ut.add_chronology_entry(add_unit_inputs, val_entry, clean_valor, table, param_name)
    elif "taxonname" in val_entry:
        ut.add_taxon_entry(add_unit_inputs, val_entry, clean_valor)
    elif param_name == "contactname":
        if isinstance(clean_valor, str):
            add_unit_inputs[param_name] = [v.strip() for v in clean_valor.split("|") if v.strip()]
        else:
            add_unit_inputs[param_name] = [
                value.strip() for item in clean_valor for value in item.split("|") if value.strip()
            ]
    else:
        add_unit_inputs[param_name] = clean_valor
