import DataBUS.neotomaHelpers as nh
from DataBUS import Response, UThSeries
from DataBUS.UThSeries import UTH_PARAMS


def valid_uth_series(cur, yml_dict, csv_file, databus=None):
    """Validates and inserts uranium-thorium series data for geochronological samples.

    Validates U-Th series isotope data including isotope ratios, activities, and
    associated decay constants. Verifies decay constants exist in the database and
    creates UThSeries objects with validated parameters.

    When databus is provided and geochron IDs are available
    (databus['geochron'].id_list), replaces the placeholder geochronid values with
    the real inserted IDs and calls UThSeries.insert_to_db(cur) for each row.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data with U-Th parameters.
        csv_file (str): Path to CSV file containing U-Th series data.
        databus (dict | None): Prior validation results supplying geochron IDs.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_uth_series(cursor, config_dict, "uth_series_data.csv")
        Response(valid=[True, True], message=[...], validAll=True)
    """
    response = Response()
    params = UTH_PARAMS
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.uraniumseries")
        if all(value is None for value in inputs.values()):
            response.valid.append(True)
            response.message.append("✔ No U-Th series parameters provided, skipping validation.")
            return response
        indices = [i for i, value in enumerate(inputs["decayconstantid"]) if value is not None]
        inputs = {
            k: [v for i, v in enumerate(inputs[k]) if i in indices]
            if isinstance(inputs[k], list)
            else inputs[k]
            for k in inputs
        }
        if databus.get("geochron"):
            geochron_ids = databus["geochron"].id_list
            geochron_idx = databus["geochron"].indices
            common = set(indices) & set(geochron_idx)
            common_positions = [i for i, val in enumerate(geochron_idx) if val in common]
            inputs["geochronid"] = [geochron_ids[i] for i in common_positions]
            response.valid.append(True)
        else:
            response.valid.append(False)
            inputs["geochronid"] = [i + 1 for i in range(len(inputs["decayconstantid"]))]
            response.message.append(
                "✗ Geochron IDs not found in databus, using placeholder IDs for validation."
            )
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ U-Th series parameters cannot be properly extracted: {e}.")
        return response
    decay_query = """SELECT decayconstantid FROM ndb.decayconstants
                     WHERE LOWER(decayconstant) = %(decayconstant)s;"""
    for row in zip(*inputs.values(), strict=False):
        uth = dict(zip(inputs.keys(), row, strict=False))
        if (
            isinstance(uth.get("decayconstantid"), str)
            and uth["decayconstantid"].strip().lower() == "none"
        ):
            uth["decayconstantid"] = None
        if isinstance(uth.get("decayconstantid"), str):
            n = uth.get("decayconstantid")
            cur.execute(decay_query, {"decayconstant": uth.get("decayconstantid").lower().strip()})
            uth["decayconstantid"] = cur.fetchone()
            if uth["decayconstantid"]:
                uth["decayconstantid"] = uth["decayconstantid"][0]
                response.valid.append(True)
                if f"✔ Decay constant {n} found in database." not in response.message:
                    response.message.append(f"✔ Decay constant {n} found in database.")
            else:
                response.valid.append(False)
                if f"✗ Decay constant {n} not found in database." not in response.message:
                    response.message.append(f"✗ Decay constant {n} not found in database.")
        try:
            uth_obj = UThSeries(**uth)
            response.valid.append(True)
            if "✔ UThSeries can be created." not in response.message:
                response.message.append("✔ UThSeries can be created.")
            try:
                uth_obj.insert_to_db(cur)
                if "✔ UThSeries inserted." not in response.message:
                    response.message.append("✔ UThSeries inserted.")
                response.valid.append(True)
            except Exception as e:
                response.valid.append(False)
                if f"✗ UThSeries could not be inserted: {e}" not in response.message:
                    response.message.append(f"✗ UThSeries could not be inserted: {e}")
        except Exception as e:
            response.valid.append(False)
            if f"✗ UThSeries cannot be created: {e}" not in response.message:
                response.message.append(f"✗ UThSeries cannot be created: {e}")
            continue
    return response
