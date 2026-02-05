import datetime
import logging
import re
import warnings

def convert_value_by_type(value_meta, clean_value):
    """Convert a value to its specified type.

    Handles type conversions for: date, int, float, coordinates, string.
    Respects rowwise flag for bulk conversions.

    Args:
        value_meta (dict): Metadata entry containing 'type' and 'rowwise' keys.
        clean_value (str or list): Value(s) to convert.

    Returns:
        Converted value in appropriate type. Returns None for empty/NA strings.
    """
    type_spec = value_meta.get("type")
    is_rowwise = value_meta.get("rowwise")
    if type_spec == "date":
        return _convert_date(clean_value, is_rowwise)
    elif type_spec == "int":
        return _convert_int(clean_value, is_rowwise)
    elif type_spec == "float":
        return _convert_float(clean_value, is_rowwise)
    elif type_spec == "coordinates (lat,long)":
        return _convert_coordinates(clean_value)
    elif type_spec in ("string", "str"):
        return _convert_string(clean_value, is_rowwise)
    return clean_value

def _convert_date(value, is_rowwise):
    """Convert strings to date objects."""
    if is_rowwise:
        return list(map(
            lambda x: datetime.datetime.strptime(x.replace("/", "-"), "%Y-%m-%d").date(),
            value
        ))
    else:
        return datetime.datetime.strptime(value.replace("/", "-"), "%Y-%m-%d").date()

def _convert_int(value, is_rowwise):
    """Convert strings to integers."""
    return list(map(int, value)) if is_rowwise else int(value)

def _convert_float(value, is_rowwise):
    """Convert strings to floats, treating 'NA' and empty strings as None."""
    if is_rowwise:
        return [float(v) if v not in ["NA", ""] else None for v in value]
    else:
        return float(value)

def _convert_coordinates(value):
    """Convert comma-separated coordinate string to list of floats."""
    return [float(num) for num in value[0].split(",")]

def _convert_string(value, is_rowwise):
    """Convert value(s) to strings, handling NA and empty values."""
    converted = list(map(str, value)) if is_rowwise else str(value)

    if isinstance(converted, list):
        converted = [
            None if isinstance(v, str) and v.strip() in ("", "NA")
            else str(v) if not isinstance(v, list) else v
            for v in converted
        ]
        converted = None if all(item in [None, ''] for item in converted) else converted
    return converted

def prepare_parameters(params, yml_dict, table):
    """Expand parameters based on metadata subfields.

    Converts individual parameters into detailed parameter list by checking
    for subfields that match the parameter pattern in metadata (neotoma field).

    Used when values=False (default behavior).

    Args:
        params (list): Original list of parameter names.
        yml_dict (dict): Dictionary containing 'metadata' key with param definitions.
        table (str): Table name (with trailing dot if needed).

    Returns:
        list: Expanded parameter list with subfield details.
    """
    expanded_params = list(params)
    for param in params:
        subfields = [
            entry for entry in yml_dict['metadata']
            if entry.get('neotoma', '').startswith(f'{table}{param}.')
        ]
        if subfields:
            for entry in subfields:
                param_name = entry['neotoma'].replace(f'{table}', "")
                expanded_params.append(param_name)
            if param in expanded_params:
                expanded_params.remove(param)

    return expanded_params


def prepare_value_parameters(params, yml_dict):
    """Expand parameters based on column field matches.

    Converts individual parameters into detailed parameter list by checking
    for entries where the 'column' field matches the parameter name.

    Used when values=True (value column processing).

    Args:
        params (list): Original list of parameter names.
        yml_dict (dict): Dictionary containing 'metadata' key with param definitions.

    Returns:
        list: Expanded parameter list with column-matched subfield details.
    """
    expanded_params = list(params)

    for param in params:
        subfields = [
            entry for entry in yml_dict['metadata']
            if entry.get('column', '') == param
        ]
        if subfields:
            for entry in subfields:
                param_name = entry['neotoma'].replace('neotoma.', "")
                expanded_params.append(param_name)
            if param in expanded_params:
                expanded_params.remove(param)
    return expanded_params


def add_note_entry(add_unit_inputs):
    """Add a note entry to the accumulator, handling list aggregation.

    Args:
        add_unit_inputs (dict): Accumulator dictionary.
        value_meta (dict): Metadata entry.
        clean_value: The cleaned value to add.
    """
    if isinstance(add_unit_inputs.get('notes'), list):
        add_unit_inputs['notes'] = ' '.join(add_unit_inputs['notes'])


def add_chronology_entry(add_unit_inputs, value_meta, clean_value, table, i):
    """Add entry to chronologies or sampleages hierarchy.

    Args:
        add_unit_inputs (dict): Accumulator dictionary.
        value_meta (dict): Metadata entry containing chronologyname.
        clean_value: The cleaned value.
        table (str): Table name to determine key.
        i (str): Parameter name.
    """
    key = 'chronologies' if 'chronologies' in table else 'sampleages'
    if key not in add_unit_inputs:
        add_unit_inputs[key] = {}
    if clean_value is None:
        return
    if not ((isinstance(clean_value, list) and not all(x is None for x in clean_value)) or
            (not isinstance(clean_value, list) and clean_value is not None)):
        return
    if 'chronologyname' in value_meta:
        chron_name = value_meta.get('chronologyname', f'Chron_0')
        if chron_name not in add_unit_inputs[key]:
            add_unit_inputs[key][chron_name] = {}
        add_unit_inputs[key][chron_name][i] = clean_value
        if i == 'age' and key == 'chronologies':
            add_unit_inputs['chronologies'][chron_name]['isdefault'] = value_meta.get('default', False)
    else:
        k = value_meta['neotoma'].split('.')[-1]
        add_unit_inputs[k] = clean_value


def add_taxon_entry(add_unit_inputs, value_meta, clean_value):
    """Add entry to taxa hierarchy with optional unit and uncertainty info.

    Args:
        add_unit_inputs (dict): Accumulator dictionary.
        value_meta (dict): Metadata entry containing taxonname.
        clean_value: The cleaned value.
    """
    if all(x is None for x in clean_value):
        return
    taxon_name = value_meta['taxonname']
    add_unit_inputs[taxon_name] = {
        'value': clean_value
    }
    if 'unitcolumn' in value_meta:
        add_unit_inputs[taxon_name]['unitcolumn'] = value_meta['unitcolumn']
    if 'uncertaintyunit' in value_meta:
        add_unit_inputs[taxon_name]['uncertaintyunit'] = value_meta['uncertaintyunit']
    if 'uncertaintybasis' in value_meta:
        add_unit_inputs[taxon_name]['uncertaintybasis'] = value_meta['uncertaintybasis']

def finalize_output(add_unit_inputs):
    """Finalize output by cleaning up chronologies/sampleages and notes.

    Removes entries where all values are None and formats notes string.

    Args:
        add_unit_inputs (dict): Accumulator dictionary to finalize.

    Returns:
        dict: Finalized output.
    """
    # Handle chronologies/sampleages filtering
    if any(k in add_unit_inputs for k in ('chronologies', 'sampleages')):
        key = 'chronologies' if 'chronologies' in add_unit_inputs else 'sampleages'
        add_unit_inputs[key] = _filter_empty_chronologies(add_unit_inputs[key])
    return add_unit_inputs


def _filter_empty_chronologies(chron_dict):
    """Remove chronology/sampleage entries where all values are None.

    Args:
        chron_dict (dict): Dictionary of chronologies or sampleages.

    Returns:
        dict: Filtered dictionary.
    """
    expected_keys = ('ageyounger', 'ageolder', 'age', 'ageboundolder', 'ageboundyounger')
    filtered = {}
    for name, chron in chron_dict.items():
        # Check if it's a nested dict (list-like values)
        if isinstance(chron.get(list(chron.keys())[0] if chron else None), list):
            if not all(all(v is None for v in chron.get(k, [])) for k in expected_keys):
                filtered[name] = chron
        else:
            if not all(v is None for v in chron.values()):
                filtered[name] = chron
    return filtered

def retrieve_dict(yml_dict, sql_column):
    """Searches through YAML metadata for entries matching a specific SQL column using
    regex pattern matching with word boundaries.

    Examples:
        >>> yml = {'metadata': [{'neotoma': 'sites.siteid', 'type': 'int'}, {'neotoma': 'sites.sitename', 'type': 'str'}]}
        >>> retrieve_dict(yml, 'sites.siteid')
        [{'neotoma': 'sites.siteid', 'type': 'int'}]
       
    Args:
        yml_dict (dict): The YAML template object imported by the user containing 'metadata' key.
        sql_column (str): A character string indicating the SQL column to be matched.

    Returns:
        list: A list of all dictionaries associated with a particular Neotoma table/column.
    """

    try:
        assert isinstance(yml_dict, dict)
        assert yml_dict.get("metadata")
    except AssertionError:
        logging.error(
            "The yml_dict must be a dict object (not a list) containing the key 'metadata'.",
            exc_info=True,
        )
    result = [d for d in yml_dict["metadata"] if re.search(fr'\b{sql_column}\b', d["neotoma"])]
    if result is None:
        warnings.warn("No matching dictionary entry found.")
        return None
    else:
        return result
    
def clean_column(column, template, clean=True):
    """Extracts a single column from template data and optionally reduces it to unique
    values. Handles special cases where there are multiple non-empty values by raising
    an error, unless one value is empty/None.
    """
    if clean:
        return _extract_unique_column_value(template, column)
    else:
        values = [row[column] for row in template]
        return values if values else None

def _extract_unique_column_value(template, column):
    """When multiple values exist, checks if they differ only by case. If one value is
    empty/None and another is not, returns the non-empty value.
    """
    original_values = [
        row[column] if isinstance(row[column], str) else row[column]
        for row in template
    ]
    unique_original = list(set(original_values))

    lowercase_values = [
        row[column].lower() if isinstance(row[column], str) else row[column]
        for row in template
    ]
    unique_lowercase = list(set(lowercase_values))

    # All values are the same
    if len(unique_lowercase) == 1:
        return unique_original[0]

    # No values
    if len(unique_original) == 0:
        return None

    # Exactly two values, one empty and one non-empty
    if len(unique_original) == 2:
        non_empty_values = [v for v in unique_original if v not in ['', None]]
        empty_values = [v for v in unique_original if v in ['', None]]
        if non_empty_values and empty_values:
            return non_empty_values[0]

    # Multiple different non-empty values (error condition)
    raise ValueError(
        f"Multiple different values found in non-rowwise element '{column}'. "
        "Correct the template or the data."
    )