import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, CollectionUnit, Response
from DataBUS.CollectionUnit import CU_PARAMS
import re

def valid_collunit(cur, yml_dict, csv_file, databus=None):
    """Validates collection unit data for sample collection sites.

    Validates collection unit parameters including coordinates, collection date,
    depositional environment, substrate, and collection device. Handles date parsing,
    queries database for valid ID values, and detects close/duplicate collection units.

    Args:
        cur (psycopg2.extensions.connection): Database connection to Neotoma (local or remote).
        yml_dict (dict): Dictionary containing data from YAML template.
        csv_file (str): Path to CSV file with required data to upload.

    Returns:
        Response: Response object with validation results, messages, and collection unit list.

    Examples:
        >>> valid_collunit(cur, yml_dict, "data.csv")
        Response(valid=[True], message=[...])
    """
    response = Response()
    try:
        inputs = nh.pull_params(CU_PARAMS, yml_dict, csv_file, "ndb.collectionunits")
        if 'geog.latitude' in inputs and 'geog.longitude' in inputs:
            lat = inputs.pop('geog.latitude')
            lon = inputs.pop('geog.longitude')
            if lat is not None and lon is not None:
                inputs['geog'] = Geog((lat, lon))
            else:
                inputs['geog'] = None
        elif inputs.get('geog') is not None:
            inputs['geog'] = Geog(inputs['geog']) #has to be a tuple of 2 or 4 values
        else:
            inputs['geog'] = None
    except (TypeError, WrongCoordinates) as e:
        response.valid.append(False)
        response.message.append(f"✗  Invalid coordinates. {e}\n")
        return response
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  CU parameters cannot be properly extracted. {e}\n")
        return response
    ids = {"colltypeid": """SELECT colltypeid FROM ndb.collectiontypes 
                            WHERE LOWER(colltype) = %(colltype)s""",
           "depenvtid":  """SELECT depenvtid FROM ndb.depenvttypes
                            WHERE LOWER(depenvt) = %(depenvt)s""",
           "substrateid":"""SELECT rocktypeid FROM ndb.rocktypes
                            WHERE LOWER(rocktype) = %(substrate)s""" }
    for key, query in ids.items():
        if isinstance(inputs.get(key), (float, int)) or inputs.get(key) is None:
            continue
        elif isinstance(inputs.get(key), str):
            cur.execute(query, {key[:-2]: inputs[key].lower().strip()})
            result = cur.fetchone()
            if result:
                inputs[key] = result[0]
            else:
                response.valid.append(False)
                response.message.append(f"✗  No match found for {key[:-2]}: {inputs[key]}. "
                                        f"{key} will be set to None.")
                inputs[key] = None
        else:
            inputs[key] = None
            response.message.append(f"✗  {key} must be a string or number.")
            response.valid.append(False)
    try:
        cu = CollectionUnit(**inputs)
        response.valid.append(True)
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"Failed to create CollectionUnit object. {e}")
        return response
    response.message.append(f"Handlename: {cu.handle}")
    cur.execute(
        "SELECT * FROM ndb.collectionunits WHERE LOWER(handle) = %(handle)s;",
        ({"handle": cu.handle.lower()}))
    coll_info = cur.fetchone()
    if not coll_info:
        response.message.append("✔  No handle found. Creating a new collection unit.")
        response.valid.append(True)
        _check_close_handles(cu, found_cu=None, cur=cur, response=response, limit=1)
        if databus is not None:
            try:
                cu.siteid = databus['sites'].id_int
                response.id_int = cu.insert_to_db(cur)
                response.message.append(f"✔  Collection unit inserted with ID {response.id_int}.")
            except Exception as e:
                response.valid.append(False)
                response.message.append(f"✗ Failed to insert collection unit: {e}")
        return response
    # Handle exists
    response.message.append("? There is a handle with this handlename.")
    try:
        columns = [desc[0] for desc in cur.description]
        raw = dict(zip(columns, coll_info))
        inputs2 = {
            'collectionunitid': int(raw['collectionunitid']),
            'handle': raw['handle'],
            'siteid': raw['siteid'],
            'colltypeid': raw.get('colltypeid'),
            'depenvtid': raw.get('depenvtid'),
            'collunitname': raw.get('collunitname'),
            'colldate': raw.get('colldate'),
            'colldevice': raw.get('colldevice'),
            'geog': Geog([raw.get('gpslatitude'), raw.get('gpslongitude')]),
            'gpsaltitude': raw.get('gpsaltitude'),
            'gpserror': raw.get('gpserror'),
            'waterdepth': raw.get('waterdepth'),
            'substrateid': raw.get('substrateid'),
            'slopeaspect': raw.get('slopeaspect'),
            'slopeangle': raw.get('slopeangle'),
            'location': raw.get('location'),
            'notes': raw.get('notes')
        }
        found_cu = CollectionUnit(**inputs2)
    except Exception as e:
        response.message.append(f"✗  Failed to build CollectionUnit from DB: {traceback.format_exc()}")
        response.valid.append(False)
        return response
    diff = cu.compare_cu(found_cu)
    if not diff:
        response.message.append(f"✔  A correct handle with this name was found; CUID: {found_cu.collectionunitid}.")
        response.valid.append(True)
        response.id_int = found_cu.collectionunitid
        return response
    # Assess differences
    response.message.append(f"? Are CollUnits equal: {cu == found_cu}.")
    response.message.append("? Fields at the CU level differ. Verify that the information is correct.")
    for i in diff:
        response.message.append(f"{i}")
    required = nh.pull_required(CU_PARAMS, yml_dict, table="ndb.collectionunits")
    required_k = [key for key, value in required.items() if value]
    csv_nonempty_fields = [key for key, value in inputs.items() if value not in (None, 'NA')]
    found_keywords = set(
        keyword for keyword in required_k + csv_nonempty_fields
        if any(re.search(rf'CSV\s+\b{re.escape(keyword)}\b', text) for text in diff)
    )
    found_keywords.discard('geog')
    if found_keywords:
        response.message.append(f"✗  Required fields differ in Neotoma and CSV file: {found_keywords}")
        response.valid.append(False)
    else:
        response.message.append(f"?  Some fields differ, but they are not required fields; CUID: {found_cu.collectionunitid}.")
        response.valid.append(True)
        response.id_int = found_cu.collectionunitid
        _check_close_handles(cu, found_cu=found_cu, cur=cur, response=response)
    if response.id_int is None and databus is not None:
        response.id_int = 1
        response.valid.append(False)
        response.message.append("✗ No Collection Unit ID was obtained; a placeholder ID (1) will be used for downstream validation.")
    return response

def _check_close_handles(cu, found_cu, cur, response, limit=1):
    """Check for nearby collection units and append messages to response."""
    close_handles = cu.find_close_collunits(cur, limit=limit)
    if close_handles:
        response.message.append("? There are nearby sites with collection units:")
        for site in close_handles:
            msg = (f"Nearby site: {site[0]} with collection unit handle: {site[1]} "
                   f"at distance: {round(site[3], 2)} meters.")
            response.message.append(f"? {msg}")
            if found_cu is not None and site[2] != found_cu.collectionunitid:
                response.message.append(f"✗ Distance CUID: {site[2]} differs from found CUID: {found_cu.collectionunitid}.")
                response.valid.append(False)
            else:
                response.message.append(f"? CUID: {site[2]}.")
                response.valid.append(True)
    elif cu.geog is not None:
        response.message.append("✔  No nearby collection units found.")
        response.valid.append(True)
    else:
        response.message.append("?  No coordinates provided, so nearby collection units cannot be checked.")
    return response