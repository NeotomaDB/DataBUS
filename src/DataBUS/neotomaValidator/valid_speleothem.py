import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Speleothem
from DataBUS.Speleothem import SPELEOTHEM_PARAMS


def valid_speleothem(cur, yml_dict, csv_file, databus=None):
    """Validates speleothem data and inserts the record when databus is provided.

    Validates speleothem parameters including entity properties, drip type, geology,
    cover type, and land use information. Queries the database for valid values
    and creates a Speleothem object with validated parameters.

    When ``databus`` is provided, uses ``databus["sites"].id_int`` as the site ID
    and inserts the Speleothem record into ``ndb.speleothems`` via
    ``sp.insert_to_db(cur)``. The resulting speleothem ID is stored in
    ``response.id_int``.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (list[dict]): List of row dicts from the CSV file.
        databus (dict | None): Prior validation results. When not None, uses
            ``databus["sites"].id_int`` for the insert. Defaults to None.

    Returns:
        Response: Response object containing validation messages, validity list,
            overall status, and the inserted speleothem ID in ``response.id_int``.

    Examples:
        >>> valid_speleothem(cursor, config_dict, csv_rows)
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    driptype_q = """SELECT speleothemdriptypeid
                    FROM ndb.speleothemdriptypes
                    WHERE LOWER(speleothemdriptype) = %(element)s;"""
    entitystatus_q = """SELECT entitystatusid
                        FROM ndb.speleothementitystatuses
                        WHERE LOWER(entitystatus) = %(element)s;"""
    speleothemtypes_q = """SELECT speleothemtypeid
                           FROM ndb.speleothemtypes
                           WHERE LOWER(speleothemtype) = %(element)s;"""
    covertype_q = """SELECT entitycoverid
                     FROM ndb.entitycovertypes
                     WHERE LOWER(entitycovertype) = %(element)s;"""
    landusecovertype_q = """SELECT landusecovertypeid
                            FROM ndb.landusetypes
                            WHERE LOWER(landusecovertype) = %(element)s;"""
    vegetationcovertype_q = """SELECT vegetationcovertypeid
                               FROM ndb.vegetationcovertypes
                               WHERE LOWER(vegetationcovertype) = %(element)s;"""
    rockage_q = """SELECT relativeageid
                   FROM ndb.relativeages
                   WHERE LOWER(relativeage) = %(element)s;"""
    rocktype_q = """SELECT rocktypeid
                    FROM ndb.rocktypes
                    WHERE LOWER(rocktype) = %(element)s;"""
    units_q = """SELECT variableunitsid
                 FROM ndb.variableunits
                 WHERE LOWER(variableunits) = %(element)s"""
    par = {
        "speleothemdriptypeid": [driptype_q, "speleothemdriptypeid"],
        "entitystatusid": [entitystatus_q, "entitystatusid"],
        "speleothemtypeid": [speleothemtypes_q, "speleothemtypeid"],
        "speleothemgeologyid": [rocktype_q, "speleothemgeologyid"],  # also uses rocktype_q
        "covertypeid": [covertype_q, "entitycoverid"],
        "landusecovertypeid": [landusecovertype_q, "landusecovertypeid"],
        "vegetationcovertypeid": [vegetationcovertype_q, "vegetationcovertypeid"],
        "rockageid": [rockage_q, "rockageid"],
        "rocktypeid": [rocktype_q, "rocktypeid"],
        "dripheightunitsid": [units_q, "dripheightunitsid"],
        "entitycoverunitsid": [units_q, "entitycoverunitsid"],
        "entrancedistanceunitsid": [units_q, "entrancedistanceunitsid"],
    }
    try:
        inputs = nh.pull_params(SPELEOTHEM_PARAMS, yml_dict, csv_file, "ndb.speleothems")
    except Exception as e:
        response.valid.append(False)
        response.message.append(
            f"✗ Speleothem elements in the CSV file are not properly defined.\n"
            f"Please verify the CSV file. {e}"
        )
        return response
    if all(value is None for value in inputs.values()):
        response.valid.append(True)
        response.message.append("✔ No speleothem parameters provided.")
        return response
    if inputs.get("monitoring", "").lower() == "yes":
        inputs["monitoring"] = True
    else:
        inputs["monitoring"] = False
    if isinstance(inputs.get("ref_id"), str):
        inputs["ref_id"] = list(map(int, inputs.get("ref_id", []).split(",")))
    for inp in inputs:
        if isinstance(inputs.get(inp), str) and inp.endswith("id") and inputs[inp] is not None:
            original_value = inputs[inp]
            query = par[inp][0]
            cur.execute(query, {"element": inputs[inp].lower()})
            inputs[inp] = cur.fetchone()
            if not inputs[inp]:
                response.message.append(
                    f"✗  {inp} for {original_value} not found. Does it exist in Neotoma?"
                )
                response.valid.append(False)
            else:
                inputs[inp] = inputs[inp][0]
                response.valid.append(True)
                response.message.append(f"✔  {inp} for {inputs[inp]} found.")
    try:
        siteid = databus["sites"].id_int
    except Exception as e:
        siteid = 1  # placeholder
        response.valid.append(False)
        response.message.append(f"✗ Site ID not available; using placeholder {e}.")
    try:
        sp = Speleothem(
            siteid=siteid,
            entityname=inputs.get("entityname"),
            monitoring=inputs.get("monitoring"),
            rockageid=inputs.get("rockageid"),
            entrancedistance=inputs.get("entrancedistance"),
            entrancedistanceunits=inputs.get("entrancedistanceunits"),
            speleothemtypeid=inputs.get("speleothemtypeid"),
        )
        try:
            response.id_int = sp.insert_to_db(cur)
            response.message.append(f"✔ Speleothem inserted with ID {response.id_int}.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Could not insert speleothem: {e}")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗ Speleothem Entity cannot be created: {e}")
    if response.validAll:
        response.message.append("✔ Speleothem can be created.")
    return response
