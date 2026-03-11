import DataBUS.neotomaHelpers as nh

def vocab_uploader(cur, yml_dict, csv_file, table, upload=False, logfile=[]):
    if table == "ndb.publications":
        PARAMS = []
    elif table == "ndb.taxa":
        PARAMS = ['taxoncode', 'taxonname', 'author', 'valid', 'highertaxonid',
                  'extinct', 'taxagroupid', 'publicationid', 'validatorid', 
                  'validatedate', 'notes']
    else:
        raise ValueError(f"Table '{table}' is not supported for vocab validation / upload.")
    
    inputs = nh.pull_params(PARAMS, yml_dict, csv_file, table)
    inputs = {k: v for k, v in inputs.items() if not all(x is None for x in v)}
    print(inputs)
    outputs = []
    
    if table == "ndb.taxa":
        query = """
            SELECT taxonid
            FROM ndb.taxa
            WHERE taxoncode = %(taxoncode)s
                AND taxonname = %(taxonname)s
                AND author IS NOT DISTINCT FROM %(author)s
                AND valid IS NOT DISTINCT FROM %(valid)s
                AND highertaxonid IS NOT DISTINCT FROM %(highertaxonid)s
                AND extinct IS NOT DISTINCT FROM %(extinct)s
                AND taxagroupid IS NOT DISTINCT FROM %(taxagroupid)s
                AND publicationid IS NOT DISTINCT FROM %(publicationid)s
                AND validatorid IS NOT DISTINCT FROM %(validatorid)s
                AND validatedate IS NOT DISTINCT FROM %(validatedate)s
                AND notes IS NOT DISTINCT FROM %(notes)s
        """
        id_params = {'publicationid': """SELECT publicationid
                                        FROM ndb.publications
                                        WHERE citation = %(publicationid)s""",
                     'validatorid': """SELECT contactid
                                       FROM ndb.contacts
                                       WHERE username = %(validatorid)s""", 
                     'highertaxonid': """SELECT taxonid
                                        FROM ndb.taxa
                                        WHERE taxonname = %(highertaxonid)s""",
                     'taxagroupid': """SELECT taxagroupid
                                        FROM ndb.taxagrouptypes
                                        WHERE taxagroupname = %(taxagroupid)s"""}
        
        # zip the inputs together and loop through each row to check if it exists in the database, 
        for row in zip(*inputs.values()):
            row = dict(zip(list(inputs.keys()), row, strict=False))
            for param, query_template in id_params.items():
                param_value = row.get(param)
                if isinstance(param_value, str) and param_value.strip() != "":
                    cur.execute(query_template, {param: param_value})
                    result = cur.fetchone()
                    if result is not None:
                        row[param] = result[0]
                    else:
                        logfile.append(f"Warning: '{param_value}' for parameter '{param}' does not exist in the database. It will be treated as NULL.")
                        row[param] = None
            output = row.copy()
            cur.execute(query, row)
            result = cur.fetchone()
            if result is not None:
                output['taxonid'] = result[0]
                logfile.append(f"Taxon '{row['taxonname']}' already exists in the database with taxonid {result[0]}.")
            else:
                if upload:
                    insert_query = """
                        INSERT INTO ndb.taxa (taxoncode, taxonname, author, valid, highertaxonid,
                                              extinct, taxagroupid, publicationid, validatorid, 
                                              validatedate, notes)
                        VALUES (%(taxoncode)s, %(taxonname)s, %(author)s, %(valid)s, %(highertaxonid)s,
                                %(extinct)s, %(taxagroupid)s, %(publicationid)s, %(validatorid)s, 
                                %(validatedate)s, %(notes)s)
                        RETURNING taxonid
                    """
                    cur.execute(insert_query, row)
                    new_id = cur.fetchone()[0]
                    output['taxonid'] = new_id
                    logfile.append(f"Inserted new taxon '{row['taxonname']}' with taxonid {new_id}.")
                else:
                    output['taxonid'] = None
                    logfile.append(f"Taxon '{row['taxonname']}' does not exist in the database and would be inserted if --upload were set to True.")
            outputs.append(output)
    return outputs