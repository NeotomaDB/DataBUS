import DataBUS.neotomaHelpers as nh
from DataBUS import Datum, Response, Variable


def valid_data(cur, yml_dict, csv_file, databus=None):
    """Validates paleontological data values against the Neotoma database.

    Validates data values and associated variables (taxon, units, element, context).
    Queries database for valid variable IDs, creates Variable and Datum objects
    with validated parameters. Supports both long and wide data format.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing data to validate.
        wide (bool, optional): Flag for wide format data handling. Defaults to False.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_data(cursor, config_dict, "data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    var_query = """SELECT variableelementid FROM ndb.variableelements
                    WHERE LOWER(variableelement) = %(variableelementid)s;"""
    taxon_query = """SELECT * FROM ndb.taxa
                        WHERE LOWER(taxonname) = %(taxonid)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits
                        WHERE LOWER(variableunits) = %(variableunitsid)s;"""
    context_query = """SELECT variablecontextid FROM ndb.variablecontexts
                        WHERE LOWER(variablecontext) = %(variablecontextid)s;"""

    par = {
        "taxonid": [taxon_query, "taxonid"],
        "variableelementid": [var_query, "variableelementid"],
        "variableunitsid": [units_query, "variableunitsid"],
        "variablecontextid": [context_query, "variablecontextid"],
    }
    response = Response()
    try:
        inputs = nh.pull_params(["value"], yml_dict, csv_file, "ndb.data")
        inputs2 = nh.pull_params(
            ["taxonid", "variableunitsid", "variableelementid", "variablecontextid"],
            yml_dict,
            csv_file,
            "ndb.variables",
        )
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Error pulling parameters: {e}")
        return response
    try:
        sampleids = databus["samples"].id_list
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Sample IDs not available; using placeholder: {e}")
    data = {}
    if inputs.get("value"):
        data = inputs2.copy()
        data["value"] = inputs["value"]
        # Surface samples or data from literature with no depths/unique AU.
        if len(sampleids) == 1:
            data["sampleid"] = [sampleids[0]] * len(inputs["value"])
        data = {
            k: (v if isinstance(v, list) else [v] * len(inputs["value"])) for k, v in data.items()
        }
    else:
        data = {k: v for k, v in inputs2.items() if v is not None}
        for key in inputs:
            data[key]["taxonid"] = [key] * len(inputs[key]["value"])
            data[key]["value"] = inputs[key]["value"]
            if sampleids:
                data[key]["sampleid"] = sampleids
            else:
                data[key]["sampleid"] = [
                    i + 1 for i in range(len(inputs[key]["value"]))
                ]  # placeholder
                response.valid.append(False)
        for key in list(data.keys()):
            if "value" not in data[key] or all(v is None for v in data[key]["value"]):
                data.pop(key)
        combined_data = {}
        for key in data:
            length = len(data[key]["value"])
            for k, v in data[key].items():
                if k not in combined_data:
                    combined_data[k] = []
                if v is None:
                    combined_data[k].extend([None] * length)
                else:
                    combined_data[k].extend(v if isinstance(v, list) else [v] * length)
        data = combined_data

    vals = {}
    response.id_dict = {}
    for datum in zip(*data.values(), strict=False):
        datum = dict(zip(list(data.keys()), datum, strict=False))
        txname = datum.get("taxonid")
        if txname not in response.id_dict:
            response.id_dict[txname] = []
        for param, (query, key) in par.items():
            if isinstance(datum.get(param), str) and datum[param].strip().lower() == "none":
                datum[param] = None
            if isinstance(datum.get(param), str):
                name = datum[param]
                if datum[param].lower().strip() in vals:
                    datum[param] = vals[datum[param].lower().strip()]
                else:
                    cur.execute(query, {key: datum[param].lower().strip()})
                    result = cur.fetchone()
                    if result:
                        vals[datum[param].lower().strip()] = result[0]
                        datum[param] = result[0]
                        if (
                            f"✔ The provided {param} is correct: {name} - ID: ({result[0]})"
                            not in response.message
                        ):
                            response.message.append(
                                f"✔ The provided {param} is correct: {name} - ID: ({result[0]})"
                            )
                        response.valid.append(True)
                    else:
                        if (
                            f"✗ The provided {param} with value {datum[param]} does not exist in Neotoma DB."
                            not in response.message
                        ):
                            response.message.append(
                                f"✗ The provided {param} with value {datum[param]} does not exist in Neotoma DB."
                            )
                        response.valid.append(False)
        try:
            var = Variable(**{k: v for k, v in datum.items() if k not in ("value", "sampleid")})
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            if (
                f"✗  Variable cannot be created with provided parameters: {e}"
                not in response.message
            ):
                response.message.append(
                    f"✗  Variable cannot be created with provided parameters: {e}"
                )
            if "✗  Variable ID needed to create datum for taxon." not in response.message:
                response.message.append("✗  Variable ID needed to create datum for taxon.")
        try:
            varid = var.get_id_from_db(cur)
            varid = varid[0]
            if f"✔ Variable ID retrieved from db: {varid}" not in response.message:
                response.message.append(f"✔ Variable ID retrieved from db: {varid}")
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            if f"✗  Var ID cannot be retrieved from db: {e}" not in response.message:
                response.message.append(f"✗  Var ID cannot be retrieved from db: {e}")
            continue
        try:
            d = Datum(sampleid=datum.get("sampleid"), variableid=varid, value=datum.get("value"))
            response.valid.append(True)
            try:
                d_id = d.insert_to_db(cur)
                response.id_dict[txname].append(d_id)
            except Exception as e:
                response.valid.append(False)
                if f"✗  Datum cannot be inserted: {e}" not in response.message:
                    response.message.append(f"✗  Datum cannot be inserted: {e}")
        except Exception as e:
            response.valid.append(False)
            if f"✗  Datum cannot be created: {e}" not in response.message:
                response.message.append(f"✗  Datum cannot be created: {e}")
    return response
