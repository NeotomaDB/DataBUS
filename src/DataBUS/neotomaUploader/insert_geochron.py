import DataBUS.neotomaHelpers as nh
from DataBUS import Geochron, Response

def insert_geochron(cur, yml_dict, csv_file, uploader):
    """
    """
    response = Response()

    params = ["geochrontypeid", "agetype", 
              "age", "errorolder", "erroryounger",
              "infinite", "delta13c", "labnumber", 
              "materialdated", "notes"]

    # SISAL TERMS
    sisal_t = {'MC-ICP-MS U/Th': 'Uranium series',
               'ICP-MS U/Th Other': 'Uranium series',
               'Alpha U/Th': 'Uranium series',
               'TIMS': 'TIMS',
               'U/Th unspecified': 'Uranium series',
               'C14': 'Carbon-14'}
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.geochronology")
    indices = [i for i, value in enumerate(inputs['age']) if value is not None]
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    inputs['sampleid'] = uploader['sample_geochron'].sampleid
    try:
        assert len(inputs['sampleid']) == len(inputs['age'])
    except AssertionError:
        response.message.append("✗  Number of ages does not match number of sample IDs")
        response.valid.append(False)
    
    geochron_q = """SELECT geochrontypeid FROM ndb.geochrontypes
                    WHERE LOWER(geochrontype) = LOWER(%(geochrontype)s)"""
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    
    if inputs.get('geochrontypeid'):
        inputs['geochrontypeid'] = [sisal_t.get(item, item) for item in inputs['geochrontypeid']]
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
        kwargs = dict(zip(iterable_params.keys(), values))
        kwargs.update(static_params) 
        gc = Geochron(**kwargs)
        response.valid.append(True)
        try:
            id = gc.insert_to_db(cur)
            response.id.append(id)
            response.valid.append(True)
            response.message.append("✔  Geochronology can be inserted")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Geochronology cannot be created: {e}")
    response.validAll = all(response.valid)
    return response