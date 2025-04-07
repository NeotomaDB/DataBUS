import DataBUS.neotomaHelpers as nh
from DataBUS import Geochron, Response
from datetime import datetime

def valid_geochron(cur, yml_dict, csv_file):
    """
    """
    response = Response()

    params = ["sampleid", "geochrontypeid", "agetype", 
              "age", "errorolder", "erroryounger",
              "infinite", "delta13c", "labnumber", 
              "materialdated", "notes"]

    # SISAL TERMS
    sisal_t = {'MC-ICP-MS U/Th': 'Uranium series',
               'ICP-MS U/Th Other': 'Uranium series',
               'Alpha U/Th': 'Uranium series',
               'TIMS': 'Thermal Ionization Mass Spectrometry',
               'U/Th unspecified': 'Uranium series',
               'C14': 'Carbon-14'}
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.geochronology")
    inputs = {k: [v for v in value if v is not None and (not isinstance(v, str) or 'hiatus' not in v)
                  ] if isinstance(value, list) else value for k, value in inputs.items()
                  if value is not None and (not isinstance(value, list) or any(v is not None for v in value))}
    
    geochron_q = """SELECT geochrontypeid FROM ndb.geochrontypes
                    WHERE LOWER(geochrontype) = LOWER(%(geochrontype)s)"""
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    
    for i in range(len(inputs['geochrontypeid'])):
        entries={}
        entries['sampleid'] = 1 # Placeholder
        if inputs['geochrontypeid'][i] in sisal_t:
            inputs['geochrontypeid'][i] = sisal_t[inputs['geochrontypeid'][i]]
        cur.execute(geochron_q, {"geochrontype": inputs['geochrontypeid'][i]})
        geochronid = cur.fetchone()
        cur.execute(agetype_q, {"agetype": inputs['agetype']})
        agetypeid = cur.fetchone()
        if agetypeid:
            entries['agetypeid'] = agetypeid[0]
            response.valid.append(True)
        else:
            entries['agetypeid'] = None
            response.valid.append(False)
            response.message.append(f"Age Type {inputs['agetype'][i]} not found in database")
        if geochronid:
            entries['geochrontypeid'] = geochronid[0]
            response.valid.append(True)
        else:
            entries['geochrontypeid'] = None
            response.valid.append(False)
            response.message.append(f"Geochron Type {inputs['geochrontypeid'][i]} not found in database")
        
        entries['age'] = inputs['age'][i] if 'age' in inputs else None
        entries['errorolder'] = inputs['errorolder'][i] if 'errorolder' in inputs else None
        entries['erroryounger'] = inputs['erroryounger'][i] if 'erroryounger' in inputs else None
        entries['infinite'] = inputs['infinite'][i] if 'infinite' in inputs else None
        entries['delta13c'] = inputs['delta13c'][i] if 'delta13c' in inputs else None
        entries['labnumber'] = inputs['labnumber'][i] if 'labnumber' in inputs else None
        entries['materialdated'] = inputs['materialdated'][i] if 'materialdated' in inputs else None
        entries['notes'] = inputs['notes'][i] if 'notes' in inputs else None

        try:
            Geochron(**entries)
            response.valid.append(True)
            response.message.append("✔  Geochronology can be created")
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗  Geochronology cannot be created: {e}")
    response.validAll = all(response.valid)
    return response