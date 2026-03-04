import DataBUS.neotomaHelpers as nh
from DataBUS import Response, SampleAge
from DataBUS.SampleAge import SAMPLE_AGE_PARAMS


def valid_sample_age(cur, yml_dict, csv_file, databus=None):
    """Validates sample age data for paleontological samples.

    Validates sample age parameters including age values, uncertainty bounds, and
    age type. Handles date parsing for collection dates, validates age types against
    database, and creates SampleAge objects for each chronology. When databus is
    provided and all parameters are valid, inserts each sample age into the database
    using the real chronology ID from databus['chronologies'] and real sample IDs
    from databus['samples'].id_list.

    Args:
        cur (cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list): List of dictionaries representing CSV file data.
        databus (dict | None): Prior validation results supplying chronologyid and
            sample IDs for insert.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.

    Examples:
        >>> valid_sample_age(cursor, config_dict, csv_data)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    try:
        inputs = nh.pull_params(SAMPLE_AGE_PARAMS, yml_dict, csv_file, "ndb.sampleages")
        response.valid.append(True)
        if not inputs.get("sampleages"):
            response.valid.append(True)
            response.message.append("? No sample age parameters provided.")
            return response
        else:
            agemodel = inputs.get("agemodel")
            inputs = inputs.get("sampleages")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Sample Age parameters cannot be properly extracted. {e}")
        return response

    if databus.get("chronologies") is not None:
        chron_id_map = databus["chronologies"].name
        response.valid.append(True)
    else:
        chron_id_map = {"placeholder": 1}
        response.valid.append(False)
        response.message.append(
            "✗ No chronologies found in databus. Using placeholder chronology IDs."
        )
    if databus.get("samples") is not None:
        response.valid.append(True)
        sample_ids = databus["samples"].id_list
    else:
        response.valid.append(False)
        response.message.append(" ✗ No samples found in databus")
        return response
    for chron in inputs:
        sa = inputs[chron]
        sa["sampleid"] = sample_ids
        sa = {k: v if isinstance(v, list) else [v] for k, v in sa.items()}
        chronologyid = chron_id_map.get(chron)
        for _, row in enumerate(zip(*sa.values(), strict=False)):
            sa_age = dict(zip(sa.keys(), row, strict=False))
            sa_age["agemodel"] = agemodel
            sa_age["chronologyid"] = chronologyid
            if not sa_age.get("age"):
                continue
            if isinstance(sa_age.get("agemodel"), str) and (
                sa_age.get("agemodel").lower() == "collection date"
                or "calendar years" in sa_age.get("agemodel").lower()
            ):
                try:
                    sa_age["age"] = nh.convert_to_bp(sa_age.get("age"))
                    sa_age["ageolder"] = nh.convert_to_bp(sa_age.get("ageolder"))
                    sa_age["ageyounger"] = nh.convert_to_bp(sa_age.get("ageyounger"))
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗ Error parsing collection date: {e}")
                    continue
            try:
                sa_age.pop("agemodel")
                sa_obj = SampleAge(**sa_age)
                response.valid.append(True)
                if "✔ Sample Age is valid." not in response.message:
                    response.message.append("✔ Sample Age is valid.")
                try:
                    sa_obj.insert_to_db(cur)
                    response.valid.append(True)
                    if "✔ Sample Age inserted." not in response.message:
                        response.message.append("✔ Sample Age inserted.")
                except Exception as e:
                    response.valid.append(False)
                    if f"✗ Sample Age could not be inserted. {e}" not in response.message:
                        response.message.append(f"✗ Sample Age could not be inserted. {e}")
            except Exception as e:
                response.valid.append(False)
                if f"✗ Samples ages cannot be created. {e}" not in response.message:
                    response.message.append(f"✗ Samples ages cannot be created. {e}")
                continue
    return response
