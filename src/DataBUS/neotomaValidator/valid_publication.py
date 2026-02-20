import DataBUS.neotomaHelpers as nh
from DataBUS import Publication, Response
from DataBUS.Publication import PUB_PARAMS
from DataBUS.sqlQueries.sql_queries_publication import (
    SELECT_PUBLICATION_BY_CITATION,
    SELECT_PUBLICATION_BY_DOI,
    SELECT_PUBLICATION_BY_ID,
    SELECT_PUBLICATION_TYPE
)
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
    response = Response()
    params = PUB_PARAMS

    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.publications")
    inputs['doi'] = _flatten_list(inputs['doi'])
    inputs['publicationid'] = _flatten_list(inputs['publicationid'])
    inputs['citation'] = _flatten_list(inputs.get('citation', None), delim=' | ')

    if inputs["publicationid"]:
        inputs["publicationid"] = [value if value != "NA" else None
                                   for value in inputs["publicationid"]]
        inputs["publicationid"] = inputs["publicationid"][0]

    doi_pattern = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"

    if not inputs.get('publicationid'):
        response.message.append("? No ID present.")
        response.valid.append(True)
        if inputs.get('citation'):
            for i, cit in enumerate(inputs['citation']):
                cit = cit.strip()
                cur.execute(SELECT_PUBLICATION_BY_CITATION, {'cit': cit.lower()})
                obs = cur.fetchone()
                if obs:
                    response.message.append(f"✔  Found Publication: {obs[1]} in Neotoma.")
                    response.valid.append(True)
                else:
                    if inputs.get('doi'):
                        cur.execute(SELECT_PUBLICATION_BY_DOI, {'doi': inputs['doi'][i].lower()})
                        obs = cur.fetchone()
                        if obs:
                            response.message.append(f"✔  Found Publication: {obs[3]} in Neotoma.")
                            response.valid.append(True)
                        else:
                            response.message.append(
                                f"✗  The publication does not exist in Neotoma: "
                                f"{cit}, {inputs['doi'][i]}.")
                            response.valid.append(False)
                    else:
                        response.message.append(
                            f"✗  The publication does not exist in Neotoma: {cit}.")
                        response.valid.append(False)
        else:
            response.message.append("? No citation present. Publication info will not be uploaded.")
            response.valid.append(True)
    else:
        try:
            inputs['publicationid'] = int(inputs['publicationid'])
        except Exception:
            response.message.append("?  Publication ID is not an integer.")

        if isinstance(inputs['publicationid'], int):
            cur.execute(SELECT_PUBLICATION_BY_ID, {'pubid': inputs['publicationid']})
            pub = cur.fetchone()
            if pub:
                response.message.append(f"✔  Found Publication: {pub[3]} in Neotoma.")
                response.valid.append(True)
            else:
                response.message.append("✗  The publication does not exist in Neotoma.")
                response.valid.append(False)
        elif isinstance(inputs['publicationid'], str):
            if re.match(doi_pattern, inputs['publicationid'], re.IGNORECASE):
                response.message.append("✔  Reference is correctly formatted as DOI.")
                response.valid.append(True)
                url = f"https://api.crossref.org/works/{inputs['doi'][0]}"
                request = requests.get(url)
                if request.status_code == 200:
                    response.message.append(f"✔  DOI {inputs['doi']} found in CrossRef.")
                    response.valid.append(True)
                    data = request.json()['message']
                else:
                    response.message.append(f"✗  No DOI {inputs['doi']} found in CrossRef.")
                    data = None

                if data:
                    pub_type = data.get('type')
                    cur.execute(SELECT_PUBLICATION_TYPE, {'pub_type': pub_type.lower()})
                    pubtypeid = cur.fetchone()
                    if pubtypeid:
                        pubtypeid = pubtypeid[0]
                    Publication(pubtypeid=pubtypeid,
                                title=data['title'][0],
                                journal=data['container-title'][0],
                                vol=data['volume'],
                                issue=data['journal-issue']['issue'],
                                pages=data['page'],
                                citnumber=str(data['is-referenced-by-count']),
                                doi=data['DOI'],
                                numvol=data['volume'],
                                publisher=data['publisher'],
                                url=data['URL'],
                                origlang=data['language'])
            else:
                response.message.append("? Text found in reference column but "
                                        "it does not meet DOI standards. "
                                        "Publication info will not be uploaded.")
    return response


def _flatten_list(original_list, delim=', '):
    """Flatten a list by splitting items on a delimiter and deduplicating.

    Takes a list or string and flattens it by splitting items containing
    the delimiter character. Returns unique items as a set-derived list.

    Args:
        original_list (list or str): List or string to flatten.
        delim (str): Delimiter character to split on. Defaults to ', '.

    Returns:
        list or None: Flattened and deduplicated list, or None if input is empty list.
    """
    if isinstance(original_list, list):
        if not original_list:
            return None
        flattened = []
        for item in original_list:
            if delim in item:
                flattened.extend(item.split(delim))
            else:
                flattened.append(item)
        return list(set(flattened))
    elif isinstance(original_list, str):
        return [original_list]
    return original_list
