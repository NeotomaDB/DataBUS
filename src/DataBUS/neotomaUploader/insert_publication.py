import DataBUS.neotomaHelpers as nh
from DataBUS import Publication, Response
import requests
import re

def insert_publication(cur, yml_dict, csv_file, uploader):
    """
    Inserts a publication into the database.
    Parameters:
        cur (psycopg2.cursor): The database cursor to execute SQL commands.
        yml_dict (dict): A dictionary containing YAML configuration data.
        csv_file (str): The path to the CSV file containing publication data.
        uploader (str): The name of the uploader.
    Returns:
        Response: An object containing messages, validity status, and publication ID if applicable.
    """
    def list_flattener(original_list, delim =', '):
        flattened_list = []
        if not original_list:
            return None
        for item in original_list:
            if delim in item:
                flattened_list.extend(item.split(delim))
            else:
                flattened_list.append(item)
        flattened_list = list(set(flattened_list))
        return flattened_list
    
    response = Response()
    params = ["doi", "publicationid", "citation"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.publications")
    inputs['doi'] = list_flattener(inputs['doi'])
    inputs['publicationid'] = list_flattener(inputs['publicationid'])
    inputs['citation'] = list_flattener(inputs['citation'],  delim=' | ')
    
    if inputs["publicationid"]:
        inputs["publicationid"] = [value if value != "NA" else None for value in inputs["publicationid"]]
        inputs["publicationid"] = inputs["publicationid"][0]
        
    doi_pattern = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
    cit_q = """SELECT *, similarity(LOWER(citation), %(cit)s) as SIM
               FROM ndb.publications
               WHERE citation IS NOT NULL
                AND similarity(LOWER(citation), %(cit)s) > .65
               ORDER BY similarity(LOWER(citation), %(cit)s) DESC
               LIMIT 1; """
    
    doi_q = """SELECT *, similarity(LOWER(doi), %(doi)s) as SIM
               FROM ndb.publications
               WHERE doi IS NOT NULL
                AND similarity(LOWER(doi), %(doi)s) > .65
               ORDER BY similarity(LOWER(doi), %(doi)s) DESC
               LIMIT 1; """
    
    dataset_pub_q = """SELECT ts.insertdatasetpublication(%(datasetid)s, 
                                                          %(publicationid)s, 
                                                          %(primarypub)s)"""

    if inputs['publicationid'] is None:
        response.message.append(f"? No ID present")
        response.valid.append(True)
        if inputs['citation']:
            for i, cit in enumerate(inputs['citation']):
                cur.execute(cit_q, {'cit': cit.lower()})
                obs = cur.fetchone()
                pub_id = obs if obs is not None else None
                if pub_id:
                    response.message.append(f"✔  Found Publication: "
                                            f"{obs[3]} in Neotoma")
                    response.valid.append(True)
                    cur.execute(dataset_pub_q, {'datasetid': uploader["datasets"].datasetid,
                                                'publicationid': pub_id[0],
                                                'primarypub': True})
                else:
                    if inputs['doi'][i]:
                        cur.execute(doi_q, {'doi': inputs['doi'][i].lower()})
                        obs = cur.fetchone()
                        pub_id = obs if obs is not None else None
                        if pub_id:
                            response.message.append(f"✔  Found Publication: "
                                                    f"{obs[3]} in Neotoma")
                            response.valid.append(True)
                            cur.execute(dataset_pub_q, {'datasetid': uploader["datasets"].datasetid,
                                                        'publicationid': pub_id[0],
                                                        'primarypub': True})
                        else:
                            response.message.append(f"✗  The publication does not exist in Neotoma: {cit}, {inputs['doi'][i]}.")
                            response.valid.append(False)
                    else:
                            response.message.append(f"✗  The publication does not exist in Neotoma: {cit}.")
                            response.valid.append(False)
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
                cur.execute(dataset_pub_q, {'datasetid': uploader["datasets"].datasetid,
                                            'publicationid': inputs['publicationid'],
                                            'primarypub': True})
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
                pub_type = data['type']
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
                try:
                    pubid = pub.insert_to_db(cur)
                    response.pub = pubid
                    response.valid.append(True)
                    response.message.append(f"✔ Added Publication {pubid}.")
                except Exception as e:
                    response.message.append(f"✗  Publication Data is not correct. Error message: {e}")
                    pub = Publication()
                    pubid = pub.insert_to_db(cur)
                    response.valid.append(False)
                try:
                    cur.execute(dataset_pub_q, {'datasetid': uploader["datasetid"].datasetid,
                                                'publicationid': pubid,
                                                'primarypub': True})
                except Exception as e:
                    response.message.append("✗  Could not associate dataset ID to publication ID")
                    response.valid.append(False)
            else:
                response.message.append("? Text found in reference column but "
                                        f"it does not meet DOI standards."
                                        f"Publication info will not be uploaded.")
    response.validAll = all(response.valid)
    return response