import DataBUS.neotomaHelpers as nh
from DataBUS import Response

INSERT_DATASET_PUBLICATION = """SELECT ts.insertdatasetpublication(%(datasetid)s,
                                                          %(publicationid)s,
                                                          %(primarypub)s)"""


def valid_publication(cur, yml_dict, csv_file, databus=None):
    """Validates a publication and links it to the dataset when databus is provided.

    Validates publication information by checking against the Neotoma database.
    Accepts publication ID, DOI, or citation and performs similarity matching
    when exact matches are not found. Can also validate DOIs against CrossRef API.

    When ``databus`` is provided and ``databus["datasets"].id_int`` is available,
    calls ``ts.insertdatasetpublication`` to link the validated publication to the
    dataset. The publication ID is stored in ``response.id_int``.

    Args:
        cur (psycopg2.cursor): Database cursor to execute SQL commands.
        yml_dict (dict): Dictionary containing YAML configuration data with publication parameters.
        csv_file (list[dict]): List of row dicts from the CSV file.
        databus (dict | None): Prior validation results. When not None, uses
            ``databus["datasets"].id_int`` to insert the dataset-publication link.

    Returns:
        Response: Response object containing validation messages, validity list,
            overall validity status, and the publication ID in ``response.id_int``.

    Examples:
        >>> valid_publication(cursor, config_dict, csv_rows)
        Response(valid=[True, True], message=[...], validAll=True)
    """
    response = Response()
    params = ["doi", "citation", "publicationid"]
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.publications")
        if not any(inputs.get(param) for param in params):
            response.message.append(
                "? No publication information provided. Publication info will not be uploaded."
            )
            response.valid.append(True)
            return response
        inputs = {k: v for k, v in inputs.items() if v is not None}
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Error pulling parameters: {e}")
        return response

    try:
        datasetid = databus["datasets"].id_int
        response.valid.append(True)
    except Exception as e:
        response.message.append(f"✗ Cannot retrieve Dataset ID from databus: {e}")
        response.valid.append(False)
        datasetid = 1  # placeholder
    for key in params:
        if key in inputs:
            inputs[key] = _flatten_list(inputs[key], delim=" | ")

    if inputs.get("publicationid"):
        query = """SELECT citation
                   FROM ndb.publications
                   WHERE publicationid = %(publicationid)s"""
        cur.execute(query, {"publicationid": inputs["publicationid"]})
        result = cur.fetchone()
        if result:
            response.message.append(f"✔  Found Publication: {result[0]} in Neotoma.")
            response.valid.append(True)
            response.id_list.append(inputs["publicationid"])
            try:
                cur.execute(
                    INSERT_DATASET_PUBLICATION,
                    {
                        "datasetid": datasetid,
                        "publicationid": inputs["publicationid"],
                        "primarypub": True,
                    },
                )
                response.message.append("✔  Publication linked to dataset.")
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗  Could not link publication to dataset: {e}")
                response.valid.append(False)
        else:
            response.message.append("✗  The publication does not exist in Neotoma.")
            response.valid.append(False)
        return response
    priority_params = [("doi", 2), ("citation", 1)]
    search_param = next((param for param in priority_params if inputs.get(param[0])), None)
    response.message.append(f"Using {search_param[0]} for validation.")
    queries = {
        "doi": """SELECT publicationid FROM ndb.publications
                         WHERE LOWER(doi) = LOWER(%(ref)s)""",
        "citation": """SELECT publicationid FROM ndb.publications
                            WHERE LOWER(citation) = LOWER(%(ref)s)""",
    }
    query = queries[search_param[0]]
    for i, pub in enumerate(inputs.get(search_param[0], [])):
        cur.execute(query, {"ref": pub})
        result = cur.fetchone()
        if result:
            response.message.append(f"✔  Found Publication: {pub} in Neotoma.")
            response.valid.append(True)
            response.id_list.append(result[0])
            try:
                cur.execute(
                    INSERT_DATASET_PUBLICATION,
                    {"datasetid": datasetid, "publicationid": result[0], "primarypub": True},
                )
                response.message.append("✔  Publication linked to dataset.")
                response.valid.append(True)
            except Exception as e:
                response.message.append(f"✗  Could not link publication to dataset: {e}")
                response.valid.append(False)
        else:
            if search_param[1] == 2:
                response.message.append(f"?  No DOI {pub} found in Neotoma.")
                cur.execute(queries["citation"], {"ref": inputs[priority_params[1][0]][i]})
                result = cur.fetchone()
                if result:
                    response.message.append(
                        f"✔  Found Publication: {inputs[priority_params[1][0]][i]} in Neotoma."
                    )
                    response.valid.append(True)
                    response.id_list.append(result[0])
                    try:
                        cur.execute(
                            INSERT_DATASET_PUBLICATION,
                            {
                                "datasetid": datasetid,
                                "publicationid": result[0],
                                "primarypub": True,
                            },
                        )
                        response.message.append("✔  Publication linked to dataset.")
                        response.valid.append(True)
                    except Exception as e:
                        response.message.append(f"✗  Could not link publication to dataset: {e}")
                        response.valid.append(False)
                else:
                    response.message.append(
                        f"✗  The publication does not exist in Neotoma: {inputs[priority_params[1][0]][i]}."
                    )
                    response.valid.append(False)
            else:
                response.message.append(
                    f"✗  The publication does not exist in Neotoma: {inputs[search_param[0]][i]}."
                )
                response.valid.append(False)
    return response


def _flatten_list(original_list, delim=" | "):
    """Flatten a list by splitting items on a delimiter and deduplicating."""
    if isinstance(original_list, list):
        if not original_list:
            return None
        flattened = []
        for item in original_list:
            if delim in item:
                flattened.extend(item.split(delim))
            else:
                flattened.append(item)
        return list(set(flattened))
    elif isinstance(original_list, str):
        return [original_list]
    return original_list
