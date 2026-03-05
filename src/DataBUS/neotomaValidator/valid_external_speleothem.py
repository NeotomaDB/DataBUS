import DataBUS.neotomaHelpers as nh
from DataBUS import ExternalSpeleothem, Response
from DataBUS.Speleothem import EX_SP_PARAMS


def valid_external_speleothem(cur, yml_dict, csv_file, databus=None):
    """Validates external speleothem data and inserts the record when databus is provided.

    Validates external speleothem parameters including external database ID,
    external ID, and description. Queries the database for valid external
    database references and creates ExternalSpeleothem objects.

    When ``databus`` is provided, uses ``databus["speleothems"].id_int`` as the
    speleothem entity ID and inserts the record via
    ``es.insert_externalspeleothem_to_db(cur)``.

    Args:
        cur (cursor): Database cursor to query ndb.externaldatabases table.
        yml_dict (dict): Dictionary of configuration parameters from YAML file.
        csv_file (list[dict]): List of row dicts from the CSV file.
        databus (dict | None): Prior validation results. When not None, uses
            ``databus["speleothems"].id_int`` for the insert. Defaults to None.

    Returns:
        Response: Response object with validation results and messages.

    Examples:
        >>> valid_external_speleothem(cursor, config_dict, csv_rows)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    query = """SELECT extdatabaseid
               FROM ndb.externaldatabases
               WHERE LOWER(extdatabasename) = %(extdatabaseid)s;"""
    try:
        inputs = nh.pull_params(EX_SP_PARAMS, yml_dict, csv_file, "ndb.externalspeleothemdata")
        if all(value is None for value in inputs.values()):
            response.valid.append(True)
            response.message.append(
                "✔  No external speleothem parameters provided, skipping validation."
            )
            return response
    except Exception as e:
        response.message.append(f"✗  Cannot pull external speleothem parameters from CSV file. {e}")
        response.valid.append(False)
        return response

    if isinstance(inputs.get("extdatabaseid"), str):
        cur.execute(query, {"extdatabaseid": inputs.get("extdatabaseid").lower().strip()})
        result = cur.fetchone()
        if not result:
            response.message.append(
                f"✗  extdatabaseid for {inputs.get('extdatabaseid')} not found. "
                f"Does it exist in Neotoma?"
            )
            response.valid.append(False)
        else:
            inputs["extdatabaseid"] = result[0]
            response.valid.append(True)
            response.message.append(f"✔  extdatabaseid for {inputs.get('extdatabaseid')} found.")

    if isinstance(inputs.get("externaldescription"), str):
        inputs["externaldescription"] = inputs["externaldescription"].strip(", ").strip()
        response.valid.append(True)
    try:
        entityid = databus["speleothems"].id_int
    except Exception as e:
        entityid = 2  # placeholder
        response.valid.append(False)
        response.message.append(f"✗ Speleothem entity ID not available; using placeholder: {e}.")
    try:
        es = ExternalSpeleothem(
            entityid=entityid,
            externalid=inputs.get("externalid"),
            extdatabaseid=inputs.get("extdatabaseid"),
            externaldescription=inputs.get("externaldescription"),
        )
        response.valid.append(True)
        try:
            es.insert_externalspeleothem_to_db(cur)
            response.message.append("✔  ExternalSpeleothem inserted.")
        except Exception as e:
            response.message.append(f"✗  Cannot insert ExternalSpeleothem: {e}")
            response.valid.append(False)
    except Exception as e:
        response.message.append(f"✗  Cannot create ExternalSpeleothem object. {e}")
        response.valid.append(False)
    return response
