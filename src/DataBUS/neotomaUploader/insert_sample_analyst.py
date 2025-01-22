import DataBUS.neotomaHelpers as nh
from DataBUS import Response, Contact


def insert_sample_analyst(cur, yml_dict, csv_file, uploader):
    """
    Inserts sample analyst data into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample analysts.
            - 'contids' (list): List of dictionaries containing details of the analysts' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    params = ["contactid"]
    inputs = nh.pull_params(params, yml_dict, csv_file, "ndb.sampleanalysts")
    
    # Use this method to preserve order.
    inputs["contactid"] = list(dict.fromkeys(inputs["contactid"]))
    if len(inputs["contactid"]) == 1 and isinstance(inputs["contactid"][0],str):
        inputs["contactid"] = inputs["contactid"][0].split(" | ")
    inputs["contactid"] = list(dict.fromkeys(inputs["contactid"]))

    contids = nh.get_contacts(cur, inputs["contactid"])

    for i in range(len(uploader["samples"].sampleid)):
        for agent in contids:
            try:
                if agent['id']:
                    contact = Contact(contactid=int(agent["id"]), order=int(agent["order"]))
                    response.valid.append(True)
                    marker = True
                else:
                    response.valid.append(False)
                    contact = Contact(contactid=None, order=None)
                    response.message.append(f"✗ Contact {agent['name']} does not exist in Neotoma.")
                    marker = False
            except Exception as e:
                contact = (Contact(contactid=agent["id"]),)
                response.message.append(f"✗ Sample Analyst data is not correct. {e}")
                response.valid.append(False)
            if marker == True:
                try:
                    contact.insert_sample_analyst(
                        cur, sampleid=int(uploader["samples"].sampleid[i])
                    )
                    response.valid.append(True)
                    response.message.append(
                        f"✔  Sample Analyst {agent['id']} added "
                        f"for sample {uploader['samples'].sampleid[i]}."
                    )
                except:
                    response.message.append("Executed temporary query.")
                    response.valid.append(False)

    response.validAll = all(response.valid)
    return response
