import DataBUS.neotomaHelpers as nh
from DataBUS import DataUncertainty, Response
from DataBUS.DataUncertainty import DATAUNCERTAINTY_PARAMS


def valid_datauncertainty(cur, yml_dict, csv_file, databus=None):
    """Validates data uncertainty values against the Neotoma database.

    Validates uncertainty values, units, and basis information. Queries database
    for valid uncertainty basis IDs and variable unit IDs, then creates DataUncertainty
    objects with validated parameters. Supports both long and wide data formats.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration parameters.
        csv_file (str): Path to CSV file containing data to validate.
        wide (bool, optional): Flag for wide format taxa handling. Defaults to False.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_datauncertainty(cursor, config_dict, "uncertainty_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(DATAUNCERTAINTY_PARAMS, yml_dict, csv_file, "ndb.datauncertainties")
        if all(v is None for v in inputs.values()):
            response.message.append("? No Uncertainty Values to validate.")
            response.valid.append(True)
            return response
    except Exception as e:
        response.message.append(f"✗  Error pulling parameters for data uncertainty validation: {e}")
        response.valid.append(False)
        return response

    basis_query = """SELECT uncertaintybasisid FROM ndb.uncertaintybases
                     WHERE LOWER(uncertaintybasis) = %(uncertaintybasisid)s;"""
    units_query = """SELECT variableunitsid FROM ndb.variableunits
                     WHERE LOWER(variableunits) = %(uncertaintyunitid)s;"""
    par = {
        "uncertaintybasisid": [basis_query, "uncertaintybasisid"],
        "uncertaintyunitid": [units_query, "uncertaintyunitid"],
    }

    try:
        data_ids = databus["data"].id_dict
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Data IDs not available; using placeholders: {e}")
        data_ids = {}
    vals = {}

    for taxon in inputs:
        if not data_ids.get(taxon):
            response.message.append(
                f"? No associated data IDs found for taxon '{taxon}'; skipping uncertainty validation for this taxon."
            )
            continue
        else:
            inputs[taxon]["dataid"] = data_ids[taxon]
        if inputs[taxon].get("uncertaintyvalue") is None:
            response.message.append(
                f"? No uncertainty values provided for taxon '{taxon}'; skipping uncertainty validation for this taxon."
            )
            continue
        inputs[taxon]["uncertaintybasisid"] = [inputs[taxon].get("uncertaintybasisid")] * len(
            inputs[taxon].get("uncertaintyvalue")
        )
        for datum in zip(*inputs.get(taxon).values(), strict=False):
            datum = dict(zip(list(inputs[taxon].keys()), datum, strict=False))
            if datum.get("uncertaintyvalue") is None:
                continue
            for param, (query, key) in par.items():
                if isinstance(datum.get(param), str):
                    if datum[param].lower().strip() in vals:
                        datum[param] = vals[datum[param].lower().strip()]
                    else:
                        cur.execute(query, {key: datum[param].lower().strip()})
                        result = cur.fetchone()
                        if result:
                            name = datum[param]
                            vals[datum[param].lower().strip()] = result[0]
                            datum[param] = result[0]
                            if (
                                f"✔ The provided {param} is correct: {result[0]}"
                                not in response.message
                            ):
                                response.message.append(
                                    f"✔ The provided {param} ({name}) is correct: {result[0]}"
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
                du = DataUncertainty(
                    dataid=datum.get("dataid"),
                    uncertaintyvalue=datum.get("uncertaintyvalue"),
                    uncertaintyunitid=datum.get("variableunitsid"),
                    uncertaintybasisid=datum.get("uncertaintybasisid"),
                    notes=datum.get("notes"),
                )
                response.valid.append(True)
                if "✔  Datum Uncertainty can be created." not in response.message:
                    response.message.append("✔  Datum Uncertainty can be created.")
                try:
                    du.insert_to_db(cur)
                    response.valid.append(True)
                    if (
                        f"✔  Datum Uncertainty inserted into db for taxon '{taxon}'."
                        not in response.message
                    ):
                        response.message.append(
                            f"✔  Datum Uncertainty inserted into db for taxon '{taxon}'."
                        )
                except Exception as e:
                    response.valid.append(False)
                    if f"✗  Datum Uncertainty cannot be inserted: {e}" not in response.message:
                        response.message.append(f"✗  Datum Uncertainty cannot be inserted: {e}")
            except Exception as e:
                response.valid.append(False)
                if f"✗  Datum Uncertainty cannot be created: {e}" not in response.message:
                    response.message.append(f"✗  Datum Uncertainty cannot be created: {e}")
    return response
