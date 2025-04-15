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
               'TIMS': 'Thermal Ionization Mass Spectrometry',
               'U/Th unspecified': 'Uranium series',
               'C14': 'Carbon-14'}
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.geochronology")

    indices = [i for i, value in enumerate(inputs['age']) if value is not None]
    inputs = {k: [v for i, v in enumerate(inputs[k]) if i in indices] if isinstance(inputs[k], list) else value for k, value in inputs.items()}
    inputs['sampleid'] = uploader['sample_geochron'].sampleid
    geochron_q = """SELECT geochrontypeid FROM ndb.geochrontypes
                    WHERE LOWER(geochrontype) = LOWER(%(geochrontype)s)"""
    agetype_q = """SELECT agetypeid FROM ndb.agetypes
                   WHERE LOWER(agetype) = LOWER(%(agetype)s)"""
    try:
        for i in range(len(inputs['age'])):
            entries={}
            entries['sampleid'] = inputs['sampleid'][i]
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
            
            entries['age'] = inputs['age'][i] if inputs.get('age') is not None else None
            entries['errorolder'] = inputs['errorolder'][i] if inputs.get('errorolder') is not None else None
            entries['erroryounger'] = inputs['erroryounger'][i] if inputs.get('erroryounger') is not None else None
            entries['infinite'] = inputs['infinite'][i] if inputs.get('infinite') is not None else False # Placeholder
            entries['delta13c'] = inputs['delta13c'][i] if inputs.get('delta13c') is not None else None
            entries['labnumber'] = inputs['labnumber'][i] if inputs.get('labnumber') is not None else None
            entries['materialdated'] = inputs['materialdated'][i] if inputs.get('materialdated') is not None else None
            entries['notes'] = inputs['notes'][i] if inputs.get('notes') is not None else None
            try:
                gc = Geochron(**entries)
                id = gc.insert_to_db(cur)
                response.id.append(id)
                response.valid.append(True)
                response.message.append("✔  Geochronology can be created")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗  Geochronology cannot be created: {e}")
    except (KeyError, TypeError) as e:
        response.valid.append(False)
        response.message.append(f"✗  Geochronology cannot be created: No geochrontype declared in CSV file.")
    response.validAll = all(response.valid)
    response.message = list(set(response.message))
    return response