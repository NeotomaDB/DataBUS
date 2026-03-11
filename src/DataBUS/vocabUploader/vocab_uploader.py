import DataBUS.neotomaHelpers as nh

def vocab_uploader(cur, conn, yml_dict, csv_file, table, upload=False, logfile=[]):
    if table == "ndb.publications":
        PARAMS = []
    elif table == "ndb.taxa":
        PARAMS = ['taxoncode', 'taxonname', 'author', 'valid', 'highertaxonid',
                  'extinct', 'taxagroupid', 'publicationid', 'validatorid', 
                  'validatedate', 'notes']
    else:
        raise ValueError(f"Table '{table}' is not supported for vocab validation / upload.")
    
    inputs = nh.pull_params(PARAMS, yml_dict, csv_file, table)
    inputs = {k: v for k, v in inputs.items() if v is not None and not (isinstance(v, list) and all(x is None for x in v))}
    outputs = []
    
    if table == "ndb.taxa":
        id_params = {'publicationid': """SELECT publicationid
                                        FROM ndb.publications
                                        WHERE citation = %(publicationid)s""",
                     'validatorid': """SELECT contactid
                                       FROM ndb.contacts
                                       WHERE contactname = %(validatorid)s""",
                     'highertaxonid': """SELECT taxonid
                                        FROM ndb.taxa
                                        WHERE taxonname = %(highertaxonid)s""",
                     'taxagroupid': """SELECT taxagroupid
                                        FROM ndb.taxagrouptypes
                                        WHERE taxagroup= %(taxagroupid)s"""}
        for row in zip(*inputs.values()):
            row = dict(zip(list(inputs.keys()), row, strict=False))
            row = {k: row.get(k) for k in PARAMS}
            for param, query_template in id_params.items():
                param_value = row.get(param)
                if isinstance(param_value, str) and param_value.strip() != "":
                    try:
                        row[param] = int(param_value)
                    except ValueError:
                        cur.execute(query_template, {param: param_value})
                        result = cur.fetchone()
                        if result is not None:
                            row[param] = result[0]
                        else:
                            logfile.append(f"Warning: '{param_value}' for parameter '{param}' does not exist in the database. It will be treated as NULL.")
                            row[param] = None
            output = row.copy()
            optional_fields = [p for p in PARAMS if p != 'taxonname' and row.get(p) is not None]
            conditions = ["taxonname = %(taxonname)s"] + [f"{p} IS NOT DISTINCT FROM %({p})s" for p in optional_fields]
            dynamic_query = "SELECT taxonid FROM ndb.taxa WHERE " + " AND ".join(conditions)
            cur.execute(dynamic_query, row)
            result = cur.fetchone()
            if result is not None:
                output['taxonid'] = result[0]
                logfile.append(f"Taxon '{row['taxonname']}' already exists in the database with taxonid {result[0]}.")
            else:
                insert_query = """
                        SELECT ts.inserttaxon(_code := %(taxoncode)s,
                                              _name := %(taxonname)s,
                                              _author := %(author)s,
                                              _extinct := %(extinct)s,
                                              _groupid := %(taxagroupid)s,
                                              _higherid := %(highertaxonid)s,
                                              _pubid := %(publicationid)s,
                                              _validatorid := %(validatorid)s,
                                              _validatedate := %(validatedate)s,
                                              _notes := %(notes)s)
                        """
                cur.execute(insert_query, row)
                new_id = cur.fetchone()[0]
                output['taxonid'] = new_id
            if upload:
                conn.commit()
                logfile.append(f"Inserted new taxon '{row['taxonname']}' with taxonid {new_id}.")
            else: 
                conn.rollback()
                logfile.append(f"Validated new taxon '{row['taxonname']}' with taxonid {output.get('taxonid', 'N/A')}. Not uploaded to database.")
            outputs.append(output)
    return outputs