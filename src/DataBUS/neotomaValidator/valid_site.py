import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, Site, Response
from DataBUS.Site import SITE_PARAMS

def valid_site(cur, yml_dict, csv_file, insert=False):
    """Validates and inserts site information for the Neotoma database.

    Validates site details including coordinates, name, altitude, and area.
    Checks if site exists in Neotoma, finds close/matching sites, and compares
    provided data with existing database records. Handles coordinate validation
    and hemisphere determination. If 'insert' is True, inserts new site if not found.

    Args:
        cur (psycopg2.extensions.connection): Database connection to Neotoma database.
        yml_dict (dict): Dictionary containing parameters from YAML configuration.
        csv_file (str): Path to CSV file containing additional site parameters.
        insert (bool, optional): Whether to insert new sites. Defaults to False.

    Returns:
        Response: Contains validation results, site list, hemisphere info, and messages.

    Examples:
        >>> valid_site(cursor, config_dict, "mirror_lake_site.csv")
        Response(valid=[True], message=[...], validAll=True, closesites=[...])
    """
    response = Response()
    params = SITE_PARAMS
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sites")
    if 'geog.latitude' in inputs and 'geog.longitude' in inputs:
        lat = inputs.pop('geog.latitude')
        lon = inputs.pop('geog.longitude')
        inputs['geog'] = (lat, lon)
    try:
        inputs["geog"] = Geog((inputs["geog"][0], inputs["geog"][1]))
        response.message.append(f"? This site is expected to be "
                                f"in the {inputs['geog'].hemisphere} hemisphere.")
        response.valid.append(True)
    except (TypeError, WrongCoordinates) as e:
        response.valid.append(False)
        response.message.append(str(e))
        inputs['geog'] = [None, None]
    try:
        site = Site(**inputs)
        response.valid.append(True)
    except (ValueError, TypeError, Exception) as e:
        response.valid.append(False)
        response.message.append(f"✗ Cannot create Site object: {e}")
        return response

    if site.siteid is None:
        close_sites = site.find_close_sites(cur, limit=3)
        columns = [desc[0] for desc in cur.description]
        if close_sites:
            for site_data in close_sites:
                raw = dict(zip(columns, site_data))
                new_site = Site(
                    siteid = raw.get("siteid"),
                    sitename = raw.get("sitename"),
                    altitude = raw.get("altitude"),
                    area = raw.get("area"),
                    sitedescription = raw.get("sitedescription"),
                    notes = raw.get("notes"),
                    geog=Geog((raw.get("latitudenorth"), raw.get("longitudeeast"),
                               raw.get("latitudesouth"), raw.get("longitudewest")))
                )
                new_site.distance = round(raw.get("dist"), 3)
                response.nearby.append(new_site)
                response.valid.append(True)
                if (new_site.distance == 0 and
                        new_site.sitename.lower().strip() == site.sitename.lower().strip()):
                    site.siteid = new_site.siteid
                    response.siteid = new_site.siteid
                    response.elements.append(site)
                    response.valid.append(True)
                    response.message.append(
                        f"✔  Existing site {site.sitename}, ID {response.siteid}.")
                    break
            if response.siteid is None:
                response.message.append("?  One or more sites exist close to the requested site.")
                sitenames_list = [st.sitename for st in response.nearby]
                response.matched["namematch"] = any(
                    name == site.sitename for name in sitenames_list)
                response.matched["distmatch"] = any(
                    s.distance == 0 for s in response.nearby)
                response.doublematched = (
                    response.matched["namematch"] and response.matched["distmatch"])
                response.message.append(
                    f"? Site name {response.matched['namematch']}."
                    f" Locations {response.matched['distmatch']}.")
        else:
            response.valid.append(True)
            response.nearby = []
            response.message.append("✔  There are no sites close to the proposed site.")
    else:
        site_query = """SELECT * FROM ndb.sites WHERE siteid = %(siteid)s"""
        cur.execute(site_query, {"siteid": site.siteid})
        site_info = cur.fetchone()
        if not site_info:
            response.valid.append(False)
            response.message.append(f"? Site ID {site.siteid} is not currently associated "
                                    f"to a site in Neotoma.")
        else:
            columns = [desc[0] for desc in cur.description]
            raw = dict(zip(columns, site_info))
            new_site = Site(
                    siteid = raw.get("siteid"),
                    sitename = raw.get("sitename"),
                    altitude = raw.get("altitude"),
                    area = raw.get("area"),
                    sitedescription = raw.get("sitedescription"),
                    notes = raw.get("notes"),
                    geog=Geog((raw.get("latitudenorth"), raw.get("longitudeeast"),
                               raw.get("latitudesouth"), raw.get("longitudewest")))
                )
            response.message.append(
                f"✔  Site ID {new_site.siteid}, {new_site.sitename} found in Neotoma.")
            response.elements.append(new_site)
            response.siteid = new_site.siteid
            response.valid.append(True)
    if insert and site.siteid is None:
        try:
            response.siteid = site.insert_to_db(cur)
            response.elements.append(site)
            response.valid.append(True)
            response.message.append(f"✔  Added Site {site.sitename}, ID: {response.siteid}.")
        except Exception as e:
            response.message.append(f"✗  Cannot insert Site {site.sitename}: {e}")
            response.valid.append(False)
            return response
    return response