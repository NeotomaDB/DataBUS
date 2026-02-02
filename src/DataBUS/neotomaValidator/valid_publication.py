import DataBUS.neotomaHelpers as nh
from DataBUS import Publication, Response
from DataBUS.Publication import PUB_PARAMS
import requests
import re

def valid_publication(cur, yml_dict, csv_file):
    """Validates a publication for the database.

    Validates publication information by checking against the Neotoma database.
    Accepts publication ID, DOI, or citation and performs similarity matching
    when exact matches are not found. Can also validate DOIs against CrossRef API.

    Args:
        cur (psycopg2.cursor): Database cursor to execute SQL commands.
        yml_dict (dict): Dictionary containing YAML configuration data with publication parameters.
        csv_file (str): Path to CSV file containing publication data.

    Returns:
        Response: Response object containing validation messages, validity list, and overall validity status.
    
    Examples:
        >>> valid_publication(cursor, config_dict, "publication_data.csv")
        Response(valid=[True, True], message=[...], validAll=True)
    """
    def list_flattener(original_list, delim =', '):
        """Flatten a list by splitting items on a delimiter and deduplicating.

        Takes a list or string and flattens it by splitting items containing
        the delimiter character. Returns unique items as a set-derived list.

        Args:
            original_list (list or str): List or string to flatten.
            delim (str): Delimiter character to split on. Defaults to ', '.

        Returns:
            list or None: Flattened and deduplicated list, or None if input is empty list.
        """
        flattened_list = []
        if isinstance(original_list, list):
            if not original_list:
                return None
            for item in original_list:
                if delim in item:
                    flattened_list.extend(item.split(delim))
                else:
                    flattened_list.append(item)
            flattened_list = list(set(flattened_list))
            # TODO: use list comprehension and set for more Pythonic code
        elif isinstance(original_list, str):
            flattened_list = [original_list]
        return flattened_list
    
    response = Response()
    params = PUB_PARAMS
    
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.publications")
    inputs['doi'] = list_flattener(inputs['doi'])
    inputs['publicationid'] = list_flattener(inputs['publicationid'])
    inputs['citation'] = list_flattener(inputs.get('citation', None),  delim=' | ')

    if inputs["publicationid"]:
        inputs["publicationid"] = [value if value != "NA" else None for value in inputs["publicationid"]]
        inputs["publicationid"] = inputs["publicationid"][0]
    doi_pattern = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
    cit_q = """SELECT publicationid, citation, similarity(LOWER(citation), %(cit)s) as SIM
               FROM ndb.publications
               WHERE citation IS NOT NULL
               AND similarity(LOWER(citation), %(cit)s) >= .6
               ORDER BY similarity(LOWER(citation), %(cit)s) DESC
               LIMIT 1; """
    doi_q = """SELECT *, similarity(LOWER(doi), %(doi)s) as SIM
               FROM ndb.publications
               WHERE doi IS NOT NULL
                AND similarity(LOWER(doi), %(doi)s) > .60
               ORDER BY similarity(LOWER(doi), %(doi)s) DESC
               LIMIT 1; """
    if not inputs.get('publicationid', None):
        response.message.append(f"? No ID present")
        response.valid.append(True)
        if inputs.get('citation', None):
            for i, cit in enumerate(inputs['citation']):
                cit = cit.strip()
                cur.execute(cit_q, {'cit': cit.lower()})
                obs = cur.fetchone()
                pub_id = obs if obs is not None else None
                if pub_id:
                    response.message.append(f"✔  Found Publication: "
                                            f"{obs[1]} in Neotoma")
                    response.valid.append(True)
                else:
                    if inputs.get('doi', None):
                        cur.execute(doi_q, {'doi': inputs['doi'][i].lower()})
                        obs = cur.fetchone()
                        pub_id = obs if obs is not None else None
                        if pub_id:
                            response.message.append(f"✔  Found Publication: "
                                                    f"{obs[3]} in Neotoma")
                            response.valid.append(True)
                        else:
                            response.message.append(f"✗  The publication does not exist in Neotoma: {cit}, {inputs['doi'][i]}.")
                            response.valid.append(False)
                    else:
                            response.message.append(f"✗  The publication does not exist in Neotoma: {cit}.")
                            response.valid.append(False)
        else:
            response.message.append("? No citation present. Publication info will not be uploaded.")
            response.valid.append(True)
    else:
        try:
            inputs['publicationid'] = int(inputs['publicationid'])
        except Exception as e:
            response.message.append(f"?  Publication ID is not an integer.")
        if isinstance(inputs['publicationid'], int):
            pub_query = """
                        SELECT * FROM ndb.publications
                        WHERE publicationid = %(pubid)s
                        """
            cur.execute(pub_query, {'pubid': inputs['publicationid']})
            pub = cur.fetchone()
            if pub:
                response.message.append(f"✔  Found Publication: "
                                        f"{pub[3]} in Neotoma")
                response.valid.append(True)
            else:
                response.message.append("✗  The publication does not exist in Neotoma.")
                response.valid.append(False)
        elif isinstance(inputs['publicationid'], str):
            if re.match(doi_pattern, inputs['publicationid'], re.IGNORECASE):
                response.message.append(f"✔  Reference is correctly formatted as DOI.")
                response.valid.append(True)
                url = f"https://api.crossref.org/works/{inputs['doi'][0]}"
                request = requests.get(url)
                if request.status_code == 200:
                    response.message.append(f"✔  DOI {inputs['doi']} found in CrossRef")
                    response.valid.append(True)
                    data = request.json()
                    data = data['message']
                else:
                    response.message.append(f"✗  No DOI {inputs['doi']} found in CrossRef")
                    data = None
                pub_type = data.get('type')
                sql_neotoma = """SELECT pubtypeid FROM ndb.publicationtypes
                                WHERE LOWER(REPLACE(pubtype, ' ', '-')) LIKE %(pub_type)s
                                LIMIT 1"""
                cur.execute(sql_neotoma, {'pub_type': pub_type.lower()})
                pubtypeid = cur.fetchone()
                if pubtypeid:
                    pubtypeid = pubtypeid[0]

                pub = Publication(pubtypeid = pubtypeid,
                                  title = data['title'][0],
                                  journal = data['container-title'][0],
                                  vol = data['volume'],
                                  issue = data['journal-issue']['issue'],
                                  pages = data['page'],
                                  citnumber = str(data['is-referenced-by-count']),
                                  doi = data['DOI'],
                                  numvol = data['volume'],
                                  publisher = data['publisher'],
                                  url = data['URL'],
                                  origlang = data['language'])
            else:
                response.message.append("? Text found in reference column but "
                                        f"it does not meet DOI standards."
                                        f"Publication info will not be uploaded.")
    return response