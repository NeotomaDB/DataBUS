import DataBUS.neotomaHelpers as nh
from DataBUS import Repository, Response

def valid_dataset_repository(cur, yml_dict, csv_file):
    """Validates dataset repository information.

    Validates repository ID against the Neotoma database and creates a Repository
    object if valid repository information is provided.

    Args:
        cur (psycopg2.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing YAML configuration data.
        csv_file (str): Path to CSV file containing repository information.

    Returns:
        Response: Response object containing validation messages and overall validity status.
    
    Examples:
        >>> valid_dataset_repository(cursor, config_dict, "repo_data.csv")
        Response(valid=[True], message=[...], validAll=True)
    """
    response = Response()
    params = ["acronym", "repo", "notes"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.repository")

    for i in inputs:
        if not i:
            response.message.append(f"? {i} is None.")

    if not inputs["repo"]:
        response.valid.append(True)
        response.message.append(f"✔ No repository information to be added.")
    else:
        response.message.append(f"? Reposiory {inputs['repoid']} given.")
        query = """
                SELECT repository from ndb.repositoryinstitutions 
                WHERE repositoryid = %(repoid)s"""
        try:
            cur.execute(query, {"repoid": inputs["repoid"]})
            repo_name = cur.fetchone()
            if len(repo_name) == 1:
                response.message.append(f"✔ Repository found: {repo_name[0]}")
                response.valid.append(True)
            else:
                response.message.append("✗ Repo not found. Make sure Repo exists in ndb.repoinstitutions")
                response.valid.append(False)
        except Exception as e:
            response.message.append(f"✗ Error in query {e} or repository not found.")
            response.valid.append(False)

        query = """
                SELECT datasetid, repositoryid 
                FROM ndb.repositoryspecimens
                WHERE repositoryid = %(repoid)s 
                AND datasetid = %(datasetid)s;
                """
        try:
            Repository(repositoryid=inputs["repoid"])
            response.message.append(f"✔ Repository created.")
            response.valid.append(False)
        except Exception as e:
            response.message.append(f"✗ Repository cannot be created {e}.")
            response.valid.append(False)
    return response