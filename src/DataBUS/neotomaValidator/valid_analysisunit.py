import DataBUS.neotomaHelpers as nh
from DataBUS import AnalysisUnit, Response
from DataBUS.AnalysisUnit import ANALYSIS_UNIT_PARAMS


def valid_analysisunit(cur, yml_dict, csv_file, databus=None):
    """Validates analysis unit data.

    Validates analysis unit parameters including depth, thickness, facies ID,
    and other stratigraphic properties. Handles both single and multiple analysis
    units, creating AnalysisUnit objects with validated parameters.
    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
    Returns:
        Response: Response object containing validation messages, validity list,
                   overall status, and count of created analysis units.
    Examples:
        >>> valid_analysisunit(cur, yml_dict, csv_file)
        Response(valid=[True], message=[...], validAll=True, counter=1)
    """
    response = Response()
    try:
        inputs = nh.pull_params(ANALYSIS_UNIT_PARAMS, yml_dict, csv_file, "ndb.analysisunits")
    except Exception as e:
        response.valid.append(False)
        response.message.append(
            f"✗ AU elements in the CSV file are not properly inserted. "
            f"Please verify the CSV file: {e}"
        )
        return response
    if isinstance(inputs.get("depth"), list):
        response.counter = 0
        iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
        static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
        for values in zip(*iterable_params.values(), strict=False):
            try:
                kwargs = dict(zip(iterable_params.keys(), values, strict=False))
                kwargs.update(static_params)
                if isinstance(kwargs.get("faciesid"), str):
                    kwargs["faciesid"] = _resolve_faciesid(cur, kwargs["faciesid"], response)
                if databus is not None and databus.get("collunits") is not None:
                    kwargs["collectionunitid"] = databus["collunits"].id_int
                else:
                    kwargs["collectionunitid"] = 1  # placeholder
                    response.valid.append(False)
                    response.message.append(
                        "✗ Collection Unit ID is required for Analysis Unit validation."
                    )
                au = AnalysisUnit(**kwargs)
                response.valid.append(True)
                if databus is not None:
                    try:
                        auid = au.insert_to_db(cur)
                        response.id_list.append(auid)
                    except Exception as e:
                        response.valid.append(False)
                        response.message.append(f"✗ Could not insert AnalysisUnit: {e}")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ AnalysisUnit cannot be created: {e}")
            response.counter += 1
    else:
        if isinstance(inputs.get("faciesid"), str):
            inputs["faciesid"] = _resolve_faciesid(cur, inputs["faciesid"], response)
        if databus is not None and databus.get("collunits") is not None:
            inputs["collectionunitid"] = databus["collunits"].id_int
        else:
            inputs["collectionunitid"] = 1  # placeholder
            response.valid.append(False)
            response.message.append(
                "✗ Collection Unit ID is required for Analysis Unit validation."
            )
        try:
            au = AnalysisUnit(**inputs)
            response.valid.append(True)
            response.counter = 1
            if databus is not None:
                try:
                    auid = au.insert_to_db(cur)
                    response.id_list.append(auid)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ Could not insert AnalysisUnit: {e}")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ AnalysisUnit cannot be created: {e}")
            return response
    if response.validAll:
        response.message.append("✔ AnalysisUnit(s) can be created.")
    return response


def _resolve_faciesid(cur, faciesid, response):
    """Look up a facies ID in the database and update the response."""
    query = """SELECT faciesid
               FROM ndb.faciestypes
               WHERE LOWER(facies) = %(faciesid)s"""
    cur.execute(query, {"faciesid": faciesid.lower().strip()})
    result = cur.fetchone()
    if result is not None:
        if not any(
            f"✔ Facies ID {result[0]} found in database." in msg for msg in response.message
        ):
            response.message.append(f"✔ Facies ID {result[0]} found in database.")
        return result[0]
    # if the message exists, dont add it again
    if not any(f"✗ Facies ID {faciesid} not found in database." in msg for msg in response.message):
        response.message.append(f"✗ Facies ID {faciesid} not found in database.")
    response.valid.append(False)
    return None
