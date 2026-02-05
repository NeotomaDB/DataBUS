import DataBUS.neotomaHelpers as nh
from DataBUS import Geog, WrongCoordinates, Site, Response
from DataBUS.Site import SITE_PARAMS

def valid_site(cur, yml_dict, csv_file, insert = False):
    """Validates and inserts site information for the Neotoma database.

    Validates site details including coordinates, name, altitude, and area.
    Checks if site exists in Neotoma, finds close/matching sites, and compares
    provided data with existing database records. Handles coordinate validation
    and hemisphere determination. If 'insert' is True, inserts new site if not found.

    Args:
        cur (_psycopg2.extensions.connection_): Database connection to Neotoma database.
        yml_dict (dict): Dictionary containing parameters from YAML configuration.
        csv_file (str): Path to CSV file containing additional site parameters.

    Returns:
        Response: Contains validation results, site list, hemisphere info, and messages.
    
    Examples:
        >>> valid_site(cursor, config_dict, "mirror_lake_site.csv")  # doctest: +SKIP
        Response(valid=[True], message=['Site Mirror Lake validated successfully'], validAll=True, closesites=[...])
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
        response.message.append(f"? This set is expected to be "
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
        response.message.append(f"Cannot create Site object: {e}")
        return response
    if site.siteid is None:
        close_sites = site.find_close_sites(cur, limit=3)
        if close_sites:
            for site_data in close_sites:
                new_site = Site(
                    siteid=site_data[0],
                    sitename=site_data[1],
                    altitude=site_data[6],
                    area=site_data[7],
                    sitedescription=site_data[8],
                    notes=site_data[9],
                    geog=Geog((site_data[3], site_data[2],
                               site_data[5], site_data[4])))
                new_site.distance = round(site_data[13], 0)
                response.nearby.append(new_site)
                response.valid.append(True)
                if (new_site.distance == 0 and
                    new_site.sitename.lower().strip() == site.sitename.lower().strip()):
                    site.siteid = new_site.siteid
                    response.siteid = new_site.siteid
                    response.elements.append(site)
                    response.valid.append(True)
                    response.message.append(f"✔  Existing site {site.sitename}, ID {response.siteid} ")
                    # Consider if insert is True, upsert file if overwrite fields are True
                    # updated_site = site.update_site(new_site, overwrite, response)
                    # cur.execute(upsert_query)  # Defines upsert_site SQL function
                    # response.siteid = updated_site.upsert_to_db(cur)
                    # response.sitelist.append(updated_site)
                    # response.valid.append(True)
                    # response.message.append(f"✔  Updated Site {site.sitename}, ID {response.siteid}.")
                    # return response
                    break
            if response.siteid is None:
                response.message.append("?  One or more sites exist close to the requested site.")
                sitenames_list = [st.sitename for st in response.nearby]
                response.matched["namematch"] = any(
                    name == site.sitename for name in sitenames_list)
                response.matched["distmatch"] = any(
                    site.distance == 0 for site in response.nearby)
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
        site_query = """SELECT * from ndb.sites where siteid = %(siteid)s"""
        cur.execute(site_query, {"siteid": site.siteid})
        site_info = cur.fetchone()
        if not site_info:
            response.valid.append(False)
            response.message.append(f"? Site ID {site.siteid} is not currently associated "
                                    f"to a site in Neotoma.")
        else:
            new_site = Site(
                    siteid=site_data[0],
                    sitename=site_data[1],
                    altitude=site_data[6],
                    area=site_data[7],
                    sitedescription=site_data[8],
                    notes=site_data[9],
                    geog=Geog((site_data[3], site_data[2],
                               site_data[5], site_data[4])))
            response.message.append(f"✔  Site ID {new_site.siteid}, {new_site.sitename} found in Neotoma.")
            response.elements.append(new_site)
            response.siteid = new_site.siteid
            response.valid.append(True)
    if (insert == True) and (site.siteid is None):
        try:
            response.siteid = site.insert_to_db(cur)
            response.elements.append(site)
            response.valid.append(True)
            response.message.append(f"✔  Added Site {site.sitename}, ID: {response.siteid}.")
        except Exception as e:
            response.message.append(f"✗  Cannot insert Site {site.sitename}: {e}")
            response.valid.append(False)
            return response
    response.message = list(response.message)
    return response