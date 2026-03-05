import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Sample
from DataBUS.Sample import SAMPLE_PARAMS


def valid_sample(cur, yml_dict, csv_file, databus):
    """Validates sample data and inserts samples into the database.

    Validates sample parameters including taxon information, analysis dates, and
    preparation methods. Creates Sample objects for each analysis unit with validated
    parameters.

    Uses ``databus["analysisunits"].id_list`` for analysis unit IDs and
    ``databus["datasets"].id_int`` for the dataset ID. When analysis unit IDs are
    available, each Sample is inserted into ``ndb.samples`` and the resulting sample
    IDs are appended to ``response.id_list``.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list[dict]): List of row dicts from the CSV file.
        databus (dict): Prior validation results. Must contain
            ``databus["analysisunits"].id_list`` and ``databus["datasets"].id_int``.

    Returns:
        Response: Response object containing validation messages, validity list,
            sample counter (``counter``), and inserted sample IDs (``id_list``).

    Examples:
        >>> valid_sample(cursor, config_dict, csv_rows, databus)
        Response(valid=[True], message=[...], validAll=True, counter=5)
    """
    response = Response()
    try:
        inputs = nh.pull_params(SAMPLE_PARAMS, yml_dict, csv_file, "ndb.samples")
        try:
            inputs["analysisunitid"] = databus["analysisunits"].id_list
            response.valid.append(True)
            inputs["datasetid"] = [databus["datasets"].id_int] * len(
                databus["analysisunits"].id_list
            )
        except Exception as e:
            response.valid.append(False)
            response.message.append(
                f"✗ No analysis units found in databus. Cannot validate samples "
                f"without analysis unit IDs. Using placeholder values for "
                f"analysisunitid and datasetid: {e}."
            )
            inputs["analysisunitid"] = list(range(1, databus["analysisunits"].counter + 1))
            inputs["datasetid"] = list(range(1, databus["analysisunits"].counter + 1))
        inputs = {k: v for k, v in inputs.items() if v is not None}
    except Exception as e:
        response.message.append(f"✗ Error pulling sample parameters: {e}")
        response.valid.append(False)
        return response
    response.counter = 0
    get_taxonid = """SELECT taxonid FROM ndb.taxa
                     WHERE LOWER(taxonname) %% %(taxonname)s;"""
    for row in zip(*inputs.values(), strict=False):
        response.counter += 1
        sample = dict(zip(inputs.keys(), row, strict=False))
        if isinstance(sample.get("taxonid"), str):
            cur.execute(get_taxonid, {"taxonname": sample["taxonid"].lower().strip()})
            sample["taxonid"] = cur.fetchone()
            if sample["taxonid"]:
                sample["taxonid"] = sample["taxonid"][0]
        try:
            s = Sample(**sample)
            response.valid.append(True)
            if "✔ Sample can be created." not in response.message:
                response.message.append("✔ Sample can be created.")
            if databus.get("analysisunits") and databus["analysisunits"].id_list:
                try:
                    s_id = s.insert_to_db(cur)
                    response.id_list.append(s_id)
                except Exception as e:
                    response.valid.append(False)
                    response.message.append(f"✗  Cannot insert sample: {e}")
        except Exception as e:
            if (
                f"✗  Samples in AU ID {sample.get('analysisunitid')} is not correct: {e}"
                not in response.message
            ):
                response.message.append(
                    f"✗  Samples in AU ID {sample.get('analysisunitid')} is not correct: {e}"
                )
            response.valid.append(False)
    return response
