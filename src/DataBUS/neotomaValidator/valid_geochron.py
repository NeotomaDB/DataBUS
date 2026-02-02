import DataBUS.neotomaHelpers as nh
from DataBUS import Geochron, Response
from DataBUS.Geochron import GECHRON_PARAMS

def valid_geochron(cur, yml_dict, csv_file):
    """Validates geochronological dating data.

    Validates geochronology parameters including dating type, age, error bounds,
    and material dated. 

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing geochronology data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall status.
    
     Examples:
        >>> valid_geochron(cursor, config_dict, "dating_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    params = GECHRON_PARAMS
    
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.geochronology")
    indices = [i for i, value in enumerate(inputs['age']) if value is not None]
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    geochron_q = """SELECT geochrontypeid FROM ndb.geochrontypes
                    WHERE LOWER(geochrontype) = LOWER(%(geochrontype)s)"""
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    if inputs.get('geochrontypeid'):
        elements = set(inputs['geochrontypeid'])
        geochron = {}
        for e in elements:
            cur.execute(geochron_q, {"geochrontype": e})
            geochronid = cur.fetchone()
            if geochronid:
                geochron[e] = geochronid[0]
            else:
                response.message.append(f"✗  Geochron type {e} not found in database")
                response.valid.append(False)
                geochron[e] = None
    if inputs.get('geochrontypeid') is not None:
        inputs['geochrontypeid'] = [geochron.get(item, item) for item in inputs['geochrontypeid']]
    try:
        cur.execute(agetype_q, {"agetype": inputs['agetype']})
        agetypeid = cur.fetchone()
    except KeyError as e:
        agetypeid = None
        response.message.append("? No age type provided.")
        response.valid.append(False)
    if agetypeid:
        inputs['agetypeid'] = agetypeid[0]
        response.valid.append(True)
    else:
        inputs['agetypeid'] = None
        response.valid.append(False)
        response.message.append(f"Age Type {inputs['agetype']} not found in database")
    del inputs['agetype']
    iterable_params = {k: v for k, v in inputs.items() if isinstance(v, list)}
    static_params = {k: v for k, v in inputs.items() if not isinstance(v, list)}
    for values in zip(*iterable_params.values()):
        try:
            kwargs = dict(zip(iterable_params.keys(), values))
            kwargs.update(static_params) 
            Geochron(**kwargs)
            response.valid.append(True)
            response.message.append("✔ Geochronology created successfully.")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Geochronology cannot be created: {e}")
    response.message = list(set(response.message))
    return response