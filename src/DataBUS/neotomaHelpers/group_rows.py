from .utils import is_blank_cell


def group_rows(yml_dict, csv_file):
    """Collapse a long file to one row per analysis unit.

    A long file repeats the analysis unit on every measurement row (one row per taxon, per
    element, ...). Marking an ``ndb.analysisunits`` entry of the template with ``groupby: true``
    says that rows sharing the value of that column come from the same analysis unit, and so
    from the same sample. Several entries can be marked; rows are then grouped by the whole
    combination.

    Rows are grouped in order of first appearance. A row whose key is blank is never merged
    with another row, because a missing depth is not evidence that two rows share a unit.

    Without any ``groupby`` entry the file is returned unchanged, one unit per row.

    Args:
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list[dict]): List of row dicts from the CSV file.

    Returns:
        tuple: ``(unit_rows, row_to_unit)``. ``unit_rows`` holds the first row of every group,
            to be read with ``pull_params`` in place of ``csv_file``. ``row_to_unit[i]`` is the
            index in ``unit_rows`` (and so in the analysis unit and sample lists) of row ``i``.

    Examples:
        >>> yml = {"metadata": [{"neotoma": "ndb.analysisunits.depth", "column": "Depth",
        ...                      "groupby": True}]}
        >>> rows = [{"Depth": "10"}, {"Depth": "10"}, {"Depth": "20"}]
        >>> group_rows(yml, rows)
        ([{'Depth': '10'}, {'Depth': '20'}], [0, 0, 1])
    """
    if not isinstance(csv_file, list):
        # Multi-sheet workbooks arrive as a dict of sheets; grouping is not defined for them.
        return csv_file, None
    columns = [
        entry["column"]
        for entry in yml_dict.get("metadata") or []
        if entry.get("groupby") and "ndb.analysisunits." in entry.get("neotoma", "")
    ]
    if not columns:
        return csv_file, list(range(len(csv_file)))

    unit_rows = []
    row_to_unit = []
    seen = {}
    for row in csv_file:
        key = tuple(None if is_blank_cell(row.get(c)) else row.get(c) for c in columns)
        if None in key:
            row_to_unit.append(len(unit_rows))
            unit_rows.append(row)
        elif key in seen:
            row_to_unit.append(seen[key])
        else:
            seen[key] = len(unit_rows)
            row_to_unit.append(len(unit_rows))
            unit_rows.append(row)
    return unit_rows, row_to_unit
