import DataBUS.neotomaHelpers as nh
from DataBUS import ExternalSpeleothem, Response
from DataBUS.Speleothem import EX_SP_PARAMS


def valid_external_speleothem(cur, yml_dict, csv_file, databus=None):
    """Validates external speleothem data against the Neotoma database.

    Validates external speleothem parameters including external database ID,
    external ID, and description. Queries the database for valid external
    database references and creates ExternalSpeleothem objects.

    Args:
        cur (cursor): Database cursor to query ndb.externaldatabases table.
        yml_dict (dict): Dictionary of configuration parameters from YAML file.
        csv_file (str): Path or identifier for CSV file with external speleothem data.

    Returns:
        Response: Response object with validation results and messages.

    Examples:
        >>> valid_external_speleothem(cursor, config_dict, "ext_speleothem_data.csv")
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
    if databus is not None:
        entityid = databus["speleothems"].id_int
    else:
        entityid = 2  # placeholder
        response.valid.append(False)
        response.message.append("✗ Speleothem entity ID not available; using placeholder.")
    try:
        es = ExternalSpeleothem(
            entityid=entityid,
            externalid=inputs.get("externalid"),
            extdatabaseid=inputs.get("extdatabaseid"),
            externaldescription=inputs.get("externaldescription"),
        )
        response.valid.append(True)
        if databus is not None:
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
