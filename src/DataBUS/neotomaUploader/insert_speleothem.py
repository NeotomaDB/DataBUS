import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Speleothem
import csv
import os

def insert_speleothem(cur, yml_dict, csv_file, uploader):
    """
    Insert speleothem data into the database.
    This function extracts parameters from a provided CSV file using a YAML dictionary for
    configuration, validates and maps specific speleothem attributes (such as drip type, geology,
    status, and type) against existing entries in the database, and constructs Speleothem objects
    for insertion into the database. If multiple values exist for a parameter, the function iterates
    over the lists, creating and inserting individual speleothem records. It updates a response object
    with validation messages and status.
    Parameters:
        cur (cursor): Database cursor used to execute SQL queries.
        yml_dict (dict): Dictionary with YAML configuration parameters used for mapping CSV fields.
        csv_file (file): CSV file containing speleothem data records.
        uploader (dict): Dictionary containing uploader information; expects a 'sites' key with a 'siteid'
                         attribute to identify the site for the speleothem record.
    Returns:
        Response: An object that includes:
            - speleothem_id: The database identifier of the inserted speleothem record (if insertion succeeds).
            - valid (list): A list of booleans indicating the success or failure of each validation step.
            - message (list): A list of string messages detailing errors or confirmations during processing.
            - validAll (bool): A flag indicating whether all validations passed and the insertion was successful.
    """
    params = ['siteid', 'entityid', 'entityname', 'monitoring', 'rockageid', 'entrancedistance', 
              'entrancedistanceunitsid', 'speleothemtypeid', 'entitystatusid', 'speleothemgeologyid',
              'speleothemdriptypeid', 'dripheight', 'dripheightunitsid', 'covertypeid', 'coverthickness',
              'entitycoverunitsid', 'landusecovertypeid', 'landusecoverpercent', 'landusecovernotes',
              'vegetationcovertypeid', 'vegetationcoverpercent', 'vegetationcovernotes', 'ref_id',
              'organics', 'mineralogypetrologyfabric', 'clumpedisotopes', 'fluidinclusions', 
              'noblegastemperatures', 'c14', 'odl']
    response = Response()
    try:
        inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.speleothems")
    except Exception as e:
        response.validAll = False
        response.message.append(f"Speleothem elements in the CSV file are not properly defined.\n"
                                f"Please verify the CSV file.")
    search_q = """SELECT entityid 
                  FROM ndb.speleothems
                  WHERE entityid = %(entityid)s AND siteid = %(siteid)s;"""
    cur.execute(search_q, {'entityid': inputs['entityid'], 'siteid': uploader['sites'].siteid})
    existing = cur.fetchone()
    if existing:
        response.valid.append(True)
        response.validAll = True
        response.message.append(f"✔  Speleothem Entity with EntityID "
                                f"{inputs['entityid']} at SiteID "
                                f"{uploader['sites'].siteid} already exists.")
        response.id = existing[0]
        return response
    else:
        driptype_q = """SELECT speleothemdriptypeid 
                        FROM ndb.speleothemdriptypes
                        WHERE LOWER(speleothemdriptype) = %(element)s;"""
        entity_q = """SELECT rocktypeid 
                    FROM ndb.rocktypes
                    WHERE LOWER(rocktype) = %(element)s;"""
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
        par = {'speleothemdriptypeid': [driptype_q, 'speleothemdriptypeid'],
            'entitystatusid': [entitystatus_q, 'entitystatusid'],
            'speleothemtypeid': [speleothemtypes_q, 'speleothemtypeid'],
            'speleothemgeologyid': [entity_q, 'speleothemgeologyid'],
            'covertypeid': [covertype_q, 'entitycoverid'],
            'landusecovertypeid': [landusecovertype_q, 'landusecovertypeid'],
            'vegetationcovertypeid': [vegetationcovertype_q, 'vegetationcovertypeid'],
            'rockageid': [rockage_q, 'rockageid'],
            'rocktypeid': [rocktype_q, 'rocktypeid'],
            'dripheightunitsid': [units_q, 'dripheightunitsid'],
            'entitycoverunitsid': [units_q, 'entitycoverunitsid'],
            'entrancedistanceunitsid': [units_q, 'entrancedistanceunitsid']}

        if inputs.get('monitoring', '').lower() == 'yes':
            inputs['monitoring'] = True
        else:
            inputs['monitoring'] = False
        if isinstance(inputs.get('ref_id'), list):
            inputs['ref_id'] = list(set(inputs['ref_id']))
            elist = []
            for el in inputs['ref_id']:
                if isinstance(el, str):
                    elist.extend(el.split(','))
                else:
                    elist.append(str(el))
            inputs['ref_id'] = list(set(map(int, elist)))
        if isinstance(inputs.get('ref_id'), str):
            inputs['ref_id'] = list(map(int, inputs['ref_id'].split(',')))
        for inp in inputs:
            if inp == "fluidinclusions":
                continue
            el = inputs[inp]
            if isinstance(inputs[inp], str) and 'id' in inp:
                query = par[inp][0]
                cur.execute(query, {'element': inputs[inp].lower()})
                inputs[inp] = cur.fetchone()
                if not inputs[inp]:
                    response.message.append(f"✗  {inp} for {el} not found. "
                                            f"Does it exist in Neotoma?")
                    response.valid.append(False)
                else:
                    inputs[inp] = inputs[inp][0]
                    response.valid.append(True)
        if 'entrancedistance' not in inputs:
            inputs['entrancedistanceunitsid'] = None
        sp = Speleothem(siteid=uploader['sites'].siteid,
                        entityid=inputs['entityid'],
                        entityname=inputs.get('entityname'),
                        monitoring=inputs.get('monitoring'),
                        rockageid=inputs.get('rockageid'),
                        entrancedistance=inputs.get('entrancedistance'),
                        entrancedistanceunits=inputs.get('entrancedistanceunitsid'),
                        speleothemtypeid=inputs.get('speleothemtypeid'))
        try:
            sp.insert_to_db(cur)
            response.id = sp.entityid
            sp.insert_entitygeology_to_db(cur, id = sp.entityid,
                                        speleothemgeologyid = inputs.get('speleothemgeologyid'),
                                        notes = inputs.get('notes'))
            if 'dripheight' not in inputs:
                inputs['dripheightunitsid'] = None
            sp.insert_entitydripheight_to_db(cur, id = sp.entityid,
                                            speleothemdriptypeid = inputs.get('speleothemdriptypeid'),
                                            entitydripheight=inputs.get('dripheight'),
                                            entitydripheightunit=inputs.get('dripheightunitsid'))
            if 'coverthickness' not in inputs:
                inputs['entitycoverunitsid'] = None
            sp.insert_entitycovers_to_db(cur, id = sp.entityid,
                                        entitycoverid = inputs.get('covertypeid'),
                                        entitycoverthickness = inputs.get('coverthickness'),
                                        entitycoverunits = inputs.get('entitycoverunitsid'))
            sp.insert_entitylandusecovers_to_db(cur, id = sp.entityid, 
                                                landusecovertypeid = inputs.get('landusecovertypeid'), 
                                                landusecoverpercent = inputs.get('landusecoverpercent'), 
                                                landusecovernotes = inputs.get('landusecovernotes'))
            sp.insert_entityvegetationcovers_to_db(cur, id = sp.entityid, 
                                                vegetationcovertypeid = inputs.get('vegetationcovertypeid'), 
                                                vegetationcoverpercent = inputs.get('vegetationcoverpercent'), 
                                                vegetationcovernotes = inputs.get('vegetationcovernotes'))
            sp.insert_entitysamples_to_db(cur,
                                          organics = inputs.get('organics'),
                                          fluid_inclusions = inputs.get('fluidinclusions'),
                                          mineralogy_petrology_fabric = inputs.get('mineralogypetrologyfabric'),
                                          clumped_isotopes = inputs.get('clumpedisotopes'),
                                          noble_gas_temperatures = inputs.get('noblegastemperatures'),
                                          C14 = inputs.get('c14'),
                                          ODL = inputs.get('odl'))
            file_path = "data/references_entities.csv"
            write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
            with open(file_path, "a", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(["entity", "entitystatusid", "reference_id"])
                for i in inputs.get("ref_id", []):
                    writer.writerow([sp.entityid, inputs.get("entitystatusid"), i])
            response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Speleothem Entity cannot be inserted: " 
                                    f"{e}")
        response.message = list(set(response.message))
        response.validAll = all(response.valid)
        if response.validAll:
            response.message.append(f"✔ Speleothem uploaded ")
        return response