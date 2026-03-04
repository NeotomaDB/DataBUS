import ast

import openpyxl
import yaml


class InlineList:
    """Custom class to represent inline lists in YAML output.

    Used with custom YAML representer to output lists in flow style (inline)
    rather than block style.

    Attributes:
        data: The list data to be represented inline.
    """

    def __init__(self, data):
        """Initialize InlineList with data.

        Args:
            data: List or sequence to store.
        """
        self.data = data


def represent_inline_list(dumper, data):
    """YAML representer for InlineList objects.

    Tells the YAML dumper to output InlineList objects as inline sequences.

    Args:
        dumper: YAML dumper instance.
        data (InlineList): InlineList object to represent.

    Returns:
        YAML node representing the sequence in flow style.
    """
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data.data, flow_style=True)


yaml.add_representer(InlineList, represent_inline_list)


def excel_to_yaml(temp_file, file_name):
    """Convert Excel template file to YAML format.

    Reads data mapping and metadata from Excel sheets, processes column definitions
    including units and uncertainty information, and writes formatted YAML output.

    Examples:
        >>> excel_to_yaml('template.xlsx', 'template')  # doctest: +SKIP
        # Creates template.yml file

    Args:
        temp_file (str): Path to the Excel template file (.xls or .xlsx).
        file_name (str): Base filename for output YAML (without extension).
                        Output file will be named file_name.yml

    Returns:
        None: Writes YAML file to disk with name file_name.yml
    """
    # SUGGESTION: Extract sheet reading and data cleaning into separate functions
    wb = openpyxl.load_workbook(temp_file, data_only=True)

    def read_sheet(sheet_name):
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [str(h).lower() if h is not None else "" for h in rows[0]]
        return [dict(zip(headers, row, strict=False)) for row in rows[1:]]

    # Template info
    sheet1 = read_sheet("Data Mapping")
    sheet1 = [r for r in sheet1 if r.get("column") != "—NA—"]
    for r in sheet1:
        if isinstance(r.get("vocab"), str):
            r["vocab"] = r["vocab"].replace("'", '"').replace("\u2018", '"').replace("\u2019", '"')

    # Metadata
    metadata = [r for r in read_sheet("Metadata") if r.get("column") != "—NA—"]

    # Setting the dictionary — first row per (column, neotoma) group
    seen = set()
    data_list = []
    for record in sheet1:
        key = (record.get("column"), record.get("neotoma"))
        if key not in seen:
            seen.add(key)
            data_list.append(record)

    units_entries = []
    uncertainty_units_entries = []
    uncertainty_entries = []
    for entry in data_list:
        if entry["unitcolumn"] is None:
            del entry["unitcolumn"]
        else:
            units_dict = {
                "column": entry["unitcolumn"],
                "neotoma": "ndb.variableunits.variableunits",
                "required": False,
                "rowwise": True,
                "type": "string",
                "vocab": entry["vocab"],
            }
            units_entries.append(units_dict)
        if entry["uncertaintycolumn"] is None:
            del entry["uncertaintycolumn"]
            del entry["uncertaintybasis"]
            del entry["uncertaintyunitcolumn"]
        else:
            entry["uncertainty"] = {
                "uncertaintycolumn": entry["uncertaintycolumn"],
                "uncertaintybasis": entry["uncertaintybasis"],
                "unitcolumn": entry["uncertaintyunitcolumn"],
            }
            uncertainty_dict = {
                "column": entry["uncertaintycolumn"],
                "formatorrange": entry["formatorrange"],
                "neotoma": "ndb.values",
                "required": False,
                "rowwise": True,
                "taxonid": entry["taxonid"],
                "taxonname": entry["taxonname"],
                "type": entry["type"],
                "vocab": None,
            }
            uncertainty_entries.append(uncertainty_dict)
            del entry["uncertaintycolumn"]
            del entry["uncertaintybasis"]
            uncertainty_unit_dict = {
                "column": entry["uncertaintyunitcolumn"],
                "neotoma": "ndb.values",
                "notes": entry["notes"],
                "required": False,
                "rowwise": True,
                "type": entry["type"],
                "vocab": entry["vocab"],
            }
            uncertainty_units_entries.append(uncertainty_unit_dict)
            del entry["uncertaintyunitcolumn"]
        if entry["vocab"] is None:
            del entry["vocab"]
        else:
            if (
                isinstance(entry["vocab"], str)
                and entry["vocab"].startswith("[")
                and entry["vocab"].endswith("]")
            ):
                # Handling strings of lists for the YML
                try:
                    entry["vocab"] = ast.literal_eval(entry["vocab"])
                    entry["vocab"] = InlineList(entry["vocab"])
                except (ValueError, SyntaxError):
                    # Leave it as is
                    pass
        if entry["formatorrange"] is None:
            del entry["formatorrange"]
        if entry["constant"] is None:
            del entry["constant"]
        if entry["taxonname"] is None:
            del entry["taxonname"]
        if entry["taxonid"] is None:
            del entry["taxonid"]
        if entry["notes"] is None:
            del entry["notes"]

    # Joining it all
    data_list = data_list + units_entries + uncertainty_entries + uncertainty_units_entries
    data_list = sorted(data_list, key=lambda x: x["column"])
    data_list = metadata + data_list

    final_dict = {
        "apiVersion": "neotoma v2.0",
        "headers": 2,
        "kind": "Development",
        "metadata": data_list,
    }

    file_name = file_name + ".yml"
    with open(file_name, "w") as f:
        yaml.dump(final_dict, f)

    return None
